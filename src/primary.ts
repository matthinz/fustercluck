import { spawn } from "child_process";
import cluster, { Worker as ClusterWorker } from "cluster";
import debug from "debug";
import { EventEmitter } from "events";
import os from "os";
import { createMessageBatch, MessageBatch } from "./batch";
import {
  parsePrimaryControlMessage,
  PrimaryControlMessage,
  WorkerControlMessage,
} from "./messages";
import {
  MessageBase,
  MessageHandlers,
  Primary,
  StartOptions,
  WorkerError,
} from "./types";

type WorkerState = "initializing" | "ready" | "busy";

type PrimaryState = "not_started" | "started" | "stopping" | "stopped";

type PrimaryEvent = "error" | "receive" | "send" | "stop";

type ErrorListener = (error: WorkerError) => void;

type ReceiveListener<PrimaryMessage> = (m: PrimaryMessage) => void;

type SendListener<WorkerMessage> = (m: WorkerMessage) => void;

type StopListener = () => void;

export function startPrimary<
  PrimaryMessage extends MessageBase,
  WorkerMessage extends MessageBase
>(
  options?: StartOptions<PrimaryMessage, WorkerMessage>
): Primary<PrimaryMessage, WorkerMessage> {
  type PrimaryControlMessageWithWorkerId =
    PrimaryControlMessage<PrimaryMessage> & { workerId?: number };

  type WorkerControlMessageWithWorkerId =
    WorkerControlMessage<WorkerMessage> & { workerId?: number };

  type SentWorkerMessage = {
    message: WorkerControlMessage<WorkerMessage>;
    state: "unsent" | "sent" | "received" | "processed";
  };

  const log = debug(`fustercluck:primary`);

  const messageHandlers = {} as MessageHandlers<PrimaryMessage>;

  const messagesToProcess: PrimaryControlMessageWithWorkerId[] = [];

  const messagesToSend: WorkerControlMessageWithWorkerId[] = [];

  const sentMessagesById: {
    [id: number]: SentWorkerMessage | undefined;
  } = {};

  const sentMessageBatches: MessageBatch[] = [];

  // `nextMessageId` tracks the next ID to be assigned to a message we send.
  let nextMessageId = 0;

  // `primaryState` tracks the current state of the primary.
  let primaryState: PrimaryState = "not_started";

  // `tickImmediate` holds on to the handle of the timer used to schedule the
  // next `tick` invocation to avoid duplicate scheduling.
  let tickImmediate: NodeJS.Immediate | undefined;
  let tickTimeout: NodeJS.Timeout | undefined;
  let nextTickDelay: number | undefined;

  // `workerStates` is a dictionary linking worker id to the associated state.
  const workerStates: { [id: number]: WorkerState } = {};

  const emitter = new EventEmitter();

  // workerInitializer is an (optional) function that returns a Promise of a
  // message to send to a new worker when it comes online. The worker will
  // not be sent anymore messages until this message is processed.
  // Users can set this initializer using the `initializeWorkersWith()` function.
  let workerInitializer: (() => Promise<WorkerMessage>) | undefined;

  // stoppingPromise is a Promise that resolves once the primary has been
  // stopped and all workers have disconnected.
  let stoppingPromise: Promise<void> | undefined;

  return {
    role: "primary",
    handle,
    initializeWorkersWith,
    on,
    stop,
    sendToPrimary,
    sendToWorkers,
    sendToWorkersAndWaitForProcessing,
    sendToWorkersAndWaitForReceipt,
  };

  function handle<
    MessageType extends PrimaryMessage["type"],
    Message extends PrimaryMessage & { type: MessageType }
  >(type: MessageType, handler: (m: Message) => void | Promise<void>) {
    const handlers = messageHandlers[type] ?? [];
    handlers.push(handler as (m: PrimaryMessage) => void | Promise<void>);
    messageHandlers[type] = handlers;
  }

  function handleDisconnect(worker: ClusterWorker) {
    log(`[${worker.id}] disconnect`);

    delete workerStates[worker.id];
    spawnWorkers();
  }

  /**
   * Handles a new message coming in from a worker.
   * @param worker
   * @param input
   * @returns
   */
  function handleMessage(worker: ClusterWorker, input: unknown) {
    const message = parsePrimaryControlMessage(
      input,
      options?.parsePrimaryMessage
    );

    if (!message) {
      return;
    }

    messagesToProcess.push({
      ...message,
      workerId: worker.id,
    });

    scheduleTick("got message");
  }

  /**
   * Handles a new worker coming online. Performs any initialization required,
   * then marks the worker as "ready" to be sent messages.
   */
  function handleOnline(worker: ClusterWorker) {
    if (primaryState !== "started") {
      worker.disconnect();
      return;
    }

    const { id } = worker;

    log(`[${id}] online`);

    if (!workerInitializer) {
      // No initialization is necessary
      workerStates[id] = "ready";
      return;
    }

    workerStates[worker.id] = "initializing";

    // Send an initialization message to the worker, and wait for it to be processed.
    workerInitializer()
      .then((message) => sendToWorkerAndWaitForProcessing(id, message))
      .catch((err) => {
        log(`[${id}]: Error during initialization`, err);
        worker.disconnect();
        return false;
      })
      .then((succeeded) => {
        if (primaryState !== "started") {
          // Something happened in the interim
          worker.disconnect();
          return;
        }

        if (succeeded && worker.isConnected()) {
          workerStates[worker.id] = "ready";
          scheduleTick("worker ready");
        }
      });
  }

  /**
   * Handles receiving the SIGINT signal.
   */
  function handleSigInt() {
    if (primaryState === "stopping") {
      // We were already interrupted once, we're being asked _again_ to close.
      // Force a stop.
      stop(true);
      return;
    }

    stop();
  }

  function handleWorkerBusy(workerId: number, messageIds: number[]) {
    setWorkerState(workerId, "busy");

    if (messageIds.length === 0) {
      return;
    }

    messagesToSend.push(
      ...messageIds.map((id) => {
        const sent = sentMessagesById[id];
        if (!sent) {
          throw new Error(`Message not found by id: ${id}`);
        }
        return {
          ...sent.message,
          workerId,
        };
      })
    );

    scheduleTick("worker is busy");
  }

  /**
   * Called when a worker notifies us that it has processed one or more messages.
   * @param messageIds
   */
  function handleWorkerProcessed(messageIds: number[]) {
    for (let i = 0; i < sentMessageBatches.length; i++) {
      const batch = sentMessageBatches[i];
      batch.markProcessed(messageIds);
      if (batch.isComplete()) {
        sentMessageBatches.splice(i, 1);
        i--;
      }
    }
  }

  /**
   * Called when a worker notifies us that it has received one or more messages.
   * @param messageIds
   */
  function handleWorkerReceived(messageIds: number[]) {
    for (let i = 0; i < sentMessageBatches.length; i++) {
      const batch = sentMessageBatches[i];
      batch.markReceived(messageIds);
      if (batch.isComplete()) {
        sentMessageBatches.splice(i, 1);
        i--;
      }
    }
  }

  /**
   * Sets a new initializer to use when new workers come online.
   * @param initializer
   */
  function initializeWorkersWith(
    initializer?:
      | WorkerMessage
      | (() => WorkerMessage)
      | (() => Promise<WorkerMessage>)
      | undefined
  ) {
    if (initializer == null) {
      workerInitializer = undefined;
    } else if (typeof initializer !== "function") {
      const initializerAsMessage: WorkerMessage = initializer;
      initializer = () => Promise.resolve(initializerAsMessage);
    } else {
      const initializerAsFunc:
        | (() => WorkerMessage)
        | (() => Promise<WorkerMessage>) = initializer;
      workerInitializer = () => Promise.resolve(initializerAsFunc());
    }
  }

  function on(
    eventName: PrimaryEvent,
    listener:
      | ErrorListener
      | ReceiveListener<PrimaryMessage>
      | SendListener<WorkerMessage>
      | StopListener
  ): void {
    emitter.on(eventName, listener);
  }

  function processSystemMessage(m: PrimaryControlMessageWithWorkerId) {
    const { workerId, ...rest } = m;

    log.enabled && log(`receive from ${m.workerId}: ${JSON.stringify(rest)}`);

    if (workerId == null) {
      throw new Error(`Got ${m.type} message from null worker id?`);
    }

    switch (m.type) {
      case "worker_busy": {
        // This worker is too busy to process the things we sent it.
        handleWorkerBusy(workerId, m.messageIds);
        break;
      }
      case "worker_processed": {
        handleWorkerProcessed(m.messageIds);
        setWorkerState(workerId, m.canTakeMore ? "ready" : "busy");
        scheduleTick("worker processed");
        break;
      }
      case "worker_ready": {
        setWorkerState(workerId, "ready");
        scheduleTick("worker is ready");
        break;
      }
      case "worker_received": {
        // This worker is taking on some of the messages we've sent it!
        handleWorkerReceived(m.messageIds);
        setWorkerState(workerId, m.canTakeMore ? "ready" : "busy");
        scheduleTick("received messages");
        break;
      }
    }
  }

  function executeHandlers(m: PrimaryMessage): Promise<void> {
    const handlers = messageHandlers[m.type];
    if (!handlers || handlers.length === 0) {
      return Promise.resolve();
    }

    return handlers.reduce<Promise<void>>(
      (p, handler) =>
        p.then(() => {
          try {
            return Promise.resolve(handler(m));
          } catch (err: any) {
            return Promise.reject(err);
          }
        }),
      Promise.resolve()
    );
  }

  function scheduleTick(reason: string, delay?: number) {
    if (delay == null) {
      // Schedule w/o any delay
      if (tickTimeout) {
        clearTimeout(tickTimeout);
        tickTimeout = undefined;
        nextTickDelay = undefined;
      }
      if (!tickImmediate) {
        tickImmediate = setImmediate(tick);
        start();
      }
      return;
    }

    if (tickImmediate) {
      return;
    }

    const alreadyHaveDelay = !!tickTimeout;
    const thisDelayIsSooner = nextTickDelay == null || delay < nextTickDelay;

    if (!alreadyHaveDelay || thisDelayIsSooner) {
      if (tickTimeout) {
        clearTimeout(tickTimeout);
        tickTimeout = undefined;
      }
      tickTimeout = setTimeout(tick, delay);
      start();
    }
  }

  function sendControlMessagesToPrimary(
    messages: Omit<PrimaryControlMessage<PrimaryMessage>, "id">[]
  ) {
    if (messages.length === 0) {
      return;
    }
    messagesToProcess.push(
      ...messages.map(
        (m) =>
          ({
            ...m,
            id: nextMessageId++,
          } as PrimaryControlMessage<PrimaryMessage>)
      )
    );
    scheduleTick("send control messages to primary");
  }

  function sendControlMessagesToWorkers(
    messages: Omit<WorkerControlMessage<WorkerMessage>, "id">[]
  ): MessageBatch {
    if (messages.length === 0) {
      return createMessageBatch([]);
    }

    const messagesWithIds = messages.map((m) => ({
      ...m,
      id: nextMessageId++,
    }));

    messagesToSend.push(...messagesWithIds);

    const batch = createMessageBatch(messagesWithIds);

    sentMessageBatches.push(batch);

    scheduleTick("sendControlMessagesToWorkers");

    return batch;
  }

  /**
   * Allows the primary to send itself a message.
   * @param message
   */
  function sendToPrimary(messages: PrimaryMessage | PrimaryMessage[]) {
    sendControlMessagesToPrimary(
      (Array.isArray(messages) ? messages : [messages]).map((message) => ({
        type: "message",
        message,
      }))
    );
  }

  /**
   * Sends a batch of messages to workers and returns a Promise that resolves
   * when _all_ messages have been received by a worker.
   * @param messages
   */
  function sendToWorkers(messages: WorkerMessage[] | WorkerMessage) {
    sendControlMessagesToWorkers(
      (Array.isArray(messages) ? messages : [messages]).map((message) => ({
        type: "message",
        message,
      }))
    );
  }

  function sendToWorkersAndWaitForProcessing(
    messages: WorkerMessage[] | WorkerMessage
  ): Promise<void> {
    const batch = sendControlMessagesToWorkers(
      (Array.isArray(messages) ? messages : [messages]).map((message) => ({
        type: "message",
        message,
      }))
    );

    return batch.allProcessed();
  }

  function sendToWorkersAndWaitForReceipt(
    messages: WorkerMessage | WorkerMessage[]
  ): Promise<void> {
    const batch = sendControlMessagesToWorkers(
      (Array.isArray(messages) ? messages : [messages]).map((message) => ({
        type: "message",
        message,
      }))
    );
    return batch.allReceived();
  }

  function sendToWorkerAndWaitForProcessing(
    workerId: number,
    messages: WorkerMessage | WorkerMessage[]
  ): Promise<void> {
    const controlMessages = (
      Array.isArray(messages) ? messages : [messages]
    ).map<WorkerControlMessageWithWorkerId>((message) => ({
      workerId,
      id: nextMessageId++,
      type: "message",
      message,
    }));

    const batch = createMessageBatch(controlMessages);
    messagesToSend.push(...controlMessages);
    return batch.allProcessed();
  }

  function setPrimaryState(state: PrimaryState) {
    if (primaryState === state) {
      return;
    }
    log("%s -> %s", primaryState, state);
    primaryState = state;
  }

  function setWorkerState(workerId: number, state: WorkerState) {
    const currentState = workerStates[workerId];
    if (state === currentState) {
      return;
    }
    log(`[${workerId}] ${currentState} -> ${state}`);
    workerStates[workerId] = state;
  }

  /**
   * Forks new workers up to our limit.
   */
  function spawnWorkers() {
    if (primaryState !== "started") {
      return;
    }

    const workerCount = Object.keys(workerStates).length;
    const wanted = workersWanted();

    if (workerCount >= wanted) {
      return;
    }

    cluster.setupPrimary({
      // @ts-ignore
      serialization: "advanced",
    });

    for (let i = workerCount; i < wanted; i++) {
      cluster.fork();
    }
  }

  function start() {
    if (primaryState !== "not_started") {
      return;
    }

    cluster.on("disconnect", handleDisconnect);
    cluster.on("online", handleOnline);
    cluster.on("message", handleMessage);
    process.on("SIGINT", handleSigInt);

    setPrimaryState("started");
  }

  function stop(force?: boolean): Promise<void> {
    if (primaryState === "stopped") {
      if (!stoppingPromise) {
        throw new Error("No promise available");
      }
      return stoppingPromise;
    }

    if (primaryState === "stopping") {
      if (!stoppingPromise) {
        throw new Error("No stopping promise available");
      }

      if (force !== true) {
        // We're not forcing, so we can piggy-back on the existing stopping promise
        return stoppingPromise;
      }
    }

    setPrimaryState("stopping");

    if (force !== true && stoppingPromise) {
      return stoppingPromise;
    }

    stoppingPromise = new Promise((resolve) => {
      if (force) {
        justStop();
        return;
      }

      tryToStop();

      function tryToStop() {
        const canStop = Object.keys(workerStates).length === 0;

        if (!canStop) {
          setTimeout(tryToStop, 100);
          return;
        }

        justStop();
      }

      function justStop() {
        process.off("message", handleMessage);
        cluster.off("online", handleOnline);
        cluster.off("disconnect", handleDisconnect);

        if (tickImmediate) {
          clearImmediate(tickImmediate);
          tickImmediate = undefined;
        }

        setPrimaryState("stopped");
        resolve();
      }
    });

    return stoppingPromise;
  }

  /**
   * tick() is called _often_ to ensure new work is queued up and incoming
   * messages have been processed.
   */
  function tick() {
    tickImmediate = undefined;

    const [userMessages, systemMessages] = messagesToProcess.reduce<
      [PrimaryControlMessageWithWorkerId[], PrimaryControlMessageWithWorkerId[]]
    >(
      ([user, system], message) => {
        if (message.type === "message") {
          user.push(message);
        } else {
          system.push(message);
        }

        return [user, system];
      },
      [[], []]
    );

    messagesToProcess.splice(0, messagesToProcess.length);

    // Process system messages first, since they do things like tell us
    // what workers are currently available.
    systemMessages.forEach(processSystemMessage);

    // Then, process any messages we've received, one at a time.
    if (userMessages.length > 0) {
      userMessages
        .reduce<Promise<void>>(
          (p, message) =>
            p.then(() => {
              // (It's possible we were stopped during processing)
              if (primaryState !== "started") {
                return;
              }

              if (message.type !== "message") {
                throw new Error("Message type changed out from under us");
              }

              return executeHandlers(message.message)
                .catch((err) => {
                  log(`Error processing #${message.id}`);
                  log(err);
                })
                .then(() => {});
            }),
          Promise.resolve()
        )
        .then(() => {
          if (primaryState === "started") {
            scheduleTick("after process user messages");
          }
        });
    }

    // Finally, send messages we've got queued up to our workers.
    if (messagesToSend.length === 0) {
      return;
    }

    const allWorkerIds = Object.keys(workerStates).map((id) => Number(id));

    if (allWorkerIds.length === 0) {
      // We don't have any workers ready.
      spawnWorkers();
      scheduleTick("no workers", 500);
      return;
    }

    let readyWorkers = allWorkerIds.filter(
      (id) => workerStates[id] === "ready"
    );

    if (readyWorkers.length === 0) {
      // We don't have any workers we can send to.
      // So let's send to all of them and let them tell us whether they
      // are too busy.
      readyWorkers = allWorkerIds;
    }

    const unsendableMessages: typeof messagesToSend = [];

    messagesToSend.forEach((m) => {
      const workerId =
        m.workerId ??
        readyWorkers[Math.floor(Math.random() * readyWorkers.length)];
      const worker = cluster.workers && cluster.workers[workerId];

      if (!worker) {
        // We think we have a worker that is not actually online. This implies
        // that we've gone out-of-date w/r/t Node's cluster.
        log(`[${workerId}] worker id does not exist`);
        unsendableMessages.push(m);
        return;
      }

      log(`send to ${workerId}: ${JSON.stringify(m)}`);

      worker.send(m, (err) => {
        if (err) {
          // An error during send probably means a worker died?
          log(`[${workerId}] Error sending message to worker`, err);

          delete sentMessagesById[m.id];
          messagesToSend.push(m);

          scheduleTick("error during send to worker");
          return;
        }
      });

      sentMessagesById[m.id] = {
        message: m,
        state: "sent",
      };
    });

    messagesToSend.splice(0, messagesToSend.length);
    messagesToSend.push(...unsendableMessages);
    if (messagesToSend.length > 0) {
      scheduleTick("have messages to send");
    }
  }

  function workersWanted(): number {
    return os.cpus().length;
  }
}
