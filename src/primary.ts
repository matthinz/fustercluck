import debug from "debug";
import { EventEmitter } from "events";
import { createMessageBatch, MessageBatch } from "./batch";
import { createClusterDriver } from "./driver";
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

type WorkerState =
  | {
      state: "initializing";
      messageIds: [];
    }
  | {
      state: "ready" | "busy";
      messageIds: number[];
      idleSince?: number;
    };

type WorkerStateName = WorkerState["state"];

type PrimaryState = "not_started" | "started" | "stopping" | "stopped";

type PrimaryEvent = "error" | "receive" | "send" | "stop";

type ErrorListener = (error: WorkerError) => void;

type ReceiveListener<PrimaryMessage> = (m: PrimaryMessage) => void;

type SendListener<WorkerMessage> = (m: WorkerMessage) => void;

type StopListener = () => void;

const DEFAULT_WORKER_IDLE_TIMEOUT = 1000;

export function startPrimary<
  PrimaryMessage extends MessageBase,
  WorkerMessage extends MessageBase
>(
  options?: StartOptions<PrimaryMessage, WorkerMessage>
): Primary<PrimaryMessage, WorkerMessage> {
  type PrimaryControlMessageWithWorkerId =
    PrimaryControlMessage<PrimaryMessage> & { workerId?: string };

  type WorkerControlMessageWithWorkerId =
    WorkerControlMessage<WorkerMessage> & { workerId?: string };

  type SentWorkerMessage = {
    message: WorkerControlMessage<WorkerMessage>;
    state: "unsent" | "sent" | "received" | "processed";
  };

  const log = debug(`fustercluck:primary`);

  const driver = options?.driver ?? createClusterDriver();

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

  // `workers` is a dictionary linking worker id to the associated state.
  const workers: { [id: string]: WorkerState } = {};

  // `workersRequested` tracks now many workers we've asked to be created, but
  // haven't actually seen yet.
  let workersRequested = 0;

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
    idle,
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

  /**
   * Handles a new message coming in from a worker.
   * @param worker
   * @param input
   * @returns
   */
  function handleMessage(workerId: string, input: unknown) {
    const message = parsePrimaryControlMessage(
      input,
      options?.parsePrimaryMessage
    );

    if (!message) {
      return;
    }

    messagesToProcess.push({
      ...message,
      workerId,
    });

    scheduleTick("got message");
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

  function handleWorkerBusy(workerId: string, messageIds: number[]) {
    setWorkerState(workerId, "busy", messageIds);

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

  function handleWorkerOffline(workerId: string) {
    if (workers[workerId] == null) {
      return;
    }

    log(`[${workerId}] disconnect`);
    delete workers[workerId];

    adjustWorkers();
  }

  /**
   * Handles a new worker coming online. Performs any initialization required,
   * then marks the worker as "ready" to be sent messages.
   */
  function handleWorkerOnline(workerId: string) {
    workersRequested--;

    if (primaryState !== "started") {
      driver.takeWorkerOffline(workerId);
      return;
    }

    log(`[${workerId}] online`);

    if (!workerInitializer) {
      // No initialization is necessary
      workers[workerId] = {
        state: "ready",
        messageIds: [],
      };
      return;
    }

    workers[workerId] = { state: "initializing", messageIds: [] };

    // Send an initialization message to the worker, and wait for it to be processed.
    workerInitializer()
      .then((message) => sendToWorkerAndWaitForProcessing(workerId, message))
      .catch((err) => {
        log(`[${workerId}]: Error during initialization`, err);
        driver.takeWorkerOffline(workerId);
        return false;
      })
      .then((succeeded) => {
        if (primaryState !== "started") {
          // Something happened in the interim, take offline
          driver.takeWorkerOffline(workerId);
          return;
        }

        if (workers[workerId] == null) {
          // Our worker disappeared during initialization
          return;
        }

        if (succeeded) {
          workers[workerId] = {
            state: "ready",
            messageIds: [],
          };
          scheduleTick("worker ready");
        }
      });
  }

  /**
   * Called when a worker notifies us that it has processed one or more messages.
   * @param messageIds
   */
  function handleWorkerProcessed(
    workerId: string,
    messageIds: number[],
    canTakeMore: boolean
  ) {
    setWorkerState(workerId, canTakeMore ? "ready" : "busy", messageIds);

    for (let i = 0; i < sentMessageBatches.length; i++) {
      const batch = sentMessageBatches[i];
      batch.markProcessed(messageIds);
      if (batch.isComplete()) {
        sentMessageBatches.splice(i, 1);
        i--;
      }
    }

    adjustWorkers();

    scheduleTick("worker processed");
  }

  /**
   * Called when a worker notifies us that it has received one or more messages.
   * @param messageIds
   */
  function handleWorkerReceived(
    workerId: string,
    messageIds: number[],
    canTakeMore: boolean
  ) {
    setWorkerState(workerId, canTakeMore ? "ready" : "busy");

    for (let i = 0; i < sentMessageBatches.length; i++) {
      const batch = sentMessageBatches[i];
      batch.markReceived(messageIds);
      if (batch.isComplete()) {
        sentMessageBatches.splice(i, 1);
        i--;
      }
    }

    scheduleTick("received messages");
  }

  function idle(): Promise<void> {
    const INTERVAL = 100;
    return new Promise((resolve) => {
      tryResolve();

      function tryResolve() {
        if (sentMessageBatches.length === 0 && messagesToProcess.length === 0) {
          resolve();
        } else {
          setTimeout(tryResolve, INTERVAL);
        }
      }
    });
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
        handleWorkerProcessed(workerId, m.messageIds, m.canTakeMore);
        break;
      }
      case "worker_ready": {
        setWorkerState(workerId, "ready");
        scheduleTick("worker is ready");
        break;
      }
      case "worker_received": {
        // This worker is taking on some of the messages we've sent it!
        handleWorkerReceived(workerId, m.messageIds, m.canTakeMore);
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
    workerId: string,
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

  function setWorkerState(
    workerId: string,
    state: WorkerStateName,
    messageIdsToRemove?: number[]
  ) {
    const prev = workers[workerId];
    if (prev == null) {
      throw new Error(`Invalid worker id: ${workerId}`);
    }

    if (state !== prev.state) {
      log(`[${workerId}] ${prev.state} -> ${state}`);
    }

    if (state === "initializing") {
      throw new Error(`Can't reset worker ${workerId} to ${state}`);
    }

    workers[workerId] = {
      state,
      idleSince: prev.state === "initializing" ? undefined : prev.idleSince,
      messageIds: messageIdsToRemove
        ? prev.messageIds.filter((id) => !messageIdsToRemove.includes(id))
        : prev.messageIds,
    };
  }

  /**
   * Looks at the workers we have and tries to spin up new ones or take
   * unused ones offline.
   */
  function adjustWorkers() {
    const workerIds = Object.keys(workers);
    const workerCount = workerIds.length;
    const wanted = driver.getMaxNumberOfWorkers();

    if (primaryState === "started") {
      for (let i = workerCount + workersRequested; i < wanted; i++) {
        log("requesting new worker");
        driver.requestNewWorker();
        workersRequested++;
      }
    }

    // Prune any idle workers
    workerIds.forEach((id) => {
      const worker = workers[id];
      if (worker == null) {
        return;
      }

      if (worker.state == "ready" && worker.messageIds.length == 0) {
        if (worker.idleSince == null) {
          worker.idleSince = Date.now();
        } else {
          const idleDuration = Date.now() - worker.idleSince;

          if (
            idleDuration >
            (options?.workerIdleTimeout ?? DEFAULT_WORKER_IDLE_TIMEOUT)
          ) {
            log(
              "Worker %s has been idle for %dms. Taking offline.",
              id,
              idleDuration
            );
            driver.takeWorkerOffline(id);
            delete workers[id];
          }
        }
      }
    });
  }

  function start() {
    if (primaryState !== "not_started") {
      return;
    }

    driver.on("workerOffline", handleWorkerOffline);
    driver.on("workerOnline", handleWorkerOnline);
    driver.on("messageFromWorker", handleMessage);

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

      // Wait for all pending batches to resolve
      let promises = sentMessageBatches.reduce<Promise<void>[]>(
        (promises, batch) => {
          promises.push(batch.allProcessed());
          promises.push(batch.allReceived());
          return promises;
        },
        []
      );

      Promise.all(promises).then(() => {
        tryToStop();
      });

      function tryToStop() {
        const workersLeft = Object.keys(workers).length;
        const canStop = workersLeft === 0;

        if (!canStop) {
          log("Can't stop, %d worker(s) left", workersLeft);
          setTimeout(tryToStop, 100);
          return;
        }

        justStop();
      }

      function justStop() {
        setPrimaryState("stopped");

        if (tickImmediate) {
          clearImmediate(tickImmediate);
          tickImmediate = undefined;
        }

        driver.stop().then(resolve);
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

    const allWorkerIds = Object.keys(workers);

    let readyWorkerIds = allWorkerIds.filter(
      (id) => workers[id].state === "ready"
    );

    if (readyWorkerIds.length === 0) {
      // We don't have any workers we can send to.
      // So let's send to the busy ones so they'll tell us if they're really
      // still busy.
      readyWorkerIds = allWorkerIds.filter(
        (id) => workers[id].state === "busy"
      );
    }

    if (readyWorkerIds.length === 0) {
      // We _really_ don't have any workers ready.
      adjustWorkers();
      scheduleTick("no workers ready", 500);
      return;
    }

    const unsendableMessages: typeof messagesToSend = [];

    messagesToSend.forEach((m) => {
      const workerId =
        m.workerId ??
        readyWorkerIds[Math.floor(Math.random() * readyWorkerIds.length)];

      const worker = workers[workerId];

      if (!worker) {
        if (m.workerId) {
          // This message is meant for a worker that does not exist
          log.enabled &&
            log(
              "Message for worker %d, which does not exist: %s",
              m.workerId,
              JSON.stringify(m)
            );
          return;
        }

        throw new Error(`Invalid worker id: ${workerId}`);
      }

      if (worker.state !== "ready" && worker.state !== "busy") {
        throw new Error(`Can't send to worker ${workerId} (${worker.state})`);
      }

      log.enabled && log(`send to ${workerId}: ${JSON.stringify(m)}`);

      worker.messageIds.push(m.id);

      driver.sendToWorker(workerId, m).catch((err) => {
        delete sentMessagesById[m.id];
        messagesToSend.push(m);

        const worker = workers[workerId];
        if (worker == null) {
          log(
            `[${workerId}] Error sending message to worker (no longer exists)`,
            err
          );
        } else {
          log(`[${workerId}] Error sending message to worker`, err);
          worker.messageIds = worker.messageIds.filter((id) => id !== m.id);
        }

        scheduleTick("error during send to worker");
        return;
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
}
