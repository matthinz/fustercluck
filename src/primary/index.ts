import debug from "debug";
import { EventEmitter } from "events";
import { createClusterDriver } from "../driver";
import {
  parsePrimaryControlMessage,
  PrimaryControlMessage,
  WorkerControlMessage,
} from "../messages";
import {
  MessageBase,
  MessageHandlers,
  Primary,
  StartOptions,
  WorkerError,
} from "../types";
import { createTicker } from "../tick";
import { createSendTracker, MessageBatch } from "./sent";

type WorkerState =
  | {
      state: "initializing";
      messageIds: [];
    }
  | {
      state: "ready";
      messageIds: number[];
    }
  | { state: "busy"; messageIds: number[]; lastPokeAt: number };

type WorkerStateName = WorkerState["state"];

type PrimaryState = "not_started" | "started" | "stopping" | "stopped";

type PrimaryEvent = "error" | "receive" | "send" | "stopping";

type ErrorListener = (error: WorkerError) => void;

type ReceiveListener<PrimaryMessage> = (m: PrimaryMessage) => void;

type SendListener<WorkerMessage> = (m: WorkerMessage) => void;

type StopListener = () => void;

const BUSY_POKE_INTERVAL = 1000;

export function startPrimary<
  PrimaryMessage extends MessageBase,
  WorkerMessage extends MessageBase
>(
  options?: StartOptions<PrimaryMessage, WorkerMessage>
): Primary<PrimaryMessage, WorkerMessage> {
  type PrimaryControlMessageEnvelope = {
    fromWorkerId: string;
    message: PrimaryControlMessage<PrimaryMessage>;
  };

  type WorkerControlMessageEnvelope = {
    toWorkerId?: string;
    message: WorkerControlMessage<WorkerMessage>;
  };

  const log = debug(`fustercluck:primary`);

  const driver = options?.driver ?? createClusterDriver();

  const messageHandlers = {} as MessageHandlers<PrimaryMessage>;

  const messagesToProcess: PrimaryControlMessageEnvelope[] = [];

  const messagesToSend: WorkerControlMessageEnvelope[] = [];

  const sendTracker = createSendTracker<
    WorkerControlMessage<WorkerMessage>,
    WorkerControlMessageEnvelope
  >();

  // `nextMessageId` tracks the next ID to be assigned to a message we send.
  let nextMessageId = 0;

  // `primaryState` tracks the current state of the primary.
  let primaryState: PrimaryState = "not_started";

  // `workers` is a dictionary linking worker id to its associated state.
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

  const { scheduleTick, stopTicking } = createTicker(tick);

  return {
    role: "primary",
    handle,
    idle,
    initializeWorkersWith,
    on,
    stop,
    sendToSelf,
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
      fromWorkerId: workerId,
      message,
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

    // Since the worker can't handle these message IDs, we have to
    // recover their envelopes and re-enqueue them
    const envelopes = sendTracker.rejected(messageIds);

    messagesToSend.push(...envelopes);

    scheduleTick("worker is busy");
  }

  function handleWorkerOffline(workerId: string) {
    const worker = workers[workerId];

    if (worker == null) {
      return;
    }

    if (worker.messageIds.length > 0) {
      throw new Error(
        `Worker went offline with pending messages: ${worker.messageIds.join(
          ","
        )}`
      );
    }

    delete workers[workerId];

    log(`[${workerId}] disconnect`);

    adjustWorkers();
  }

  /**
   * Handles a new worker coming online. Performs any initialization required,
   * then marks the worker as "ready" to be sent messages.
   */
  function handleWorkerOnline(workerId: string) {
    workersRequested--;

    if (primaryState !== "started") {
      driver.takeWorkerOffline(workerId, false);
      return;
    }

    log(`[${workerId}] online`);

    scheduleTick(`worker ${workerId} came online`);

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
        driver.takeWorkerOffline(workerId, false);
        return false;
      })
      .then((succeeded) => {
        if (primaryState !== "started") {
          // Something happened in the interim, take offline
          driver.takeWorkerOffline(workerId, false);
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

    sendTracker.processed(messageIds);

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

    sendTracker.received(messageIds);

    scheduleTick("received messages");
  }

  function idle(): Promise<void> {
    const INTERVAL = 100;
    return new Promise((resolve) => {
      tryResolve();

      function tryResolve() {
        if (
          sendTracker.numberOfMessagesOutstanding() === 0 &&
          messagesToProcess.length === 0
        ) {
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

  function processSystemMessage({
    fromWorkerId,
    message,
  }: PrimaryControlMessageEnvelope) {
    log.enabled &&
      log(`receive from ${fromWorkerId}: ${JSON.stringify(message)}`);

    switch (message.type) {
      case "worker_busy": {
        // This worker is too busy to process the things we sent it.
        handleWorkerBusy(fromWorkerId, message.messageIds);
        break;
      }
      case "worker_processed": {
        handleWorkerProcessed(
          fromWorkerId,
          message.messageIds,
          message.canTakeMore
        );
        break;
      }
      case "worker_ready": {
        setWorkerState(fromWorkerId, "ready");
        scheduleTick("worker is ready");
        break;
      }
      case "worker_received": {
        // This worker is taking on some of the messages we've sent it!
        handleWorkerReceived(
          fromWorkerId,
          message.messageIds,
          message.canTakeMore
        );
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

  function sendControlMessagesToWorkers(
    messages: Omit<WorkerControlMessage<WorkerMessage>, "id">[]
  ): MessageBatch {
    if (messages.length === 0) {
      return sendTracker.createBatch([]);
    }

    const messagesWithIds = messages.map((m) => ({
      ...m,
      id: nextMessageId++,
    }));

    messagesToSend.push(
      ...messagesWithIds.map<WorkerControlMessageEnvelope>((message) => ({
        message,
      }))
    );

    scheduleTick("sendControlMessagesToWorkers");

    return sendTracker.createBatch(messagesWithIds);
  }

  function sendToSelf(messages: PrimaryMessage | PrimaryMessage[]) {
    messages = Array.isArray(messages) ? messages : [messages];
    messagesToProcess.push(
      ...messages.map<PrimaryControlMessageEnvelope>((message) => ({
        fromWorkerId: "",
        message: {
          id: nextMessageId++,
          type: "message",
          message,
        },
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
    const envelopes = (
      Array.isArray(messages) ? messages : [messages]
    ).map<WorkerControlMessageEnvelope>((message) => ({
      toWorkerId: workerId,
      message: {
        id: nextMessageId++,
        type: "message",
        message,
      },
    }));

    messagesToSend.push(...envelopes);

    const batch = sendTracker.createBatch(envelopes.map((c) => c.message));

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
    if (workerId === "") {
      // This ID is reserved for when the primary sends messages to itself
      return;
    }

    const prev = workers[workerId];
    if (prev == null) {
      throw new Error(`Invalid worker id: ${workerId}`);
    }

    if (state === prev.state) {
      if (messageIdsToRemove != null && messageIdsToRemove.length > 0) {
        prev.messageIds = prev.messageIds.filter(
          (id) => !messageIdsToRemove.includes(id)
        );
      }

      return;
    }

    if (state !== prev.state) {
      log(`[${workerId}] ${prev.state} -> ${state}`);
    }

    if (state === "initializing") {
      throw new Error(`Can't reset worker ${workerId} to ${state}`);
    }

    const messageIds = messageIdsToRemove
      ? prev.messageIds.filter((id) => !messageIdsToRemove.includes(id))
      : prev.messageIds;

    if (state === "busy") {
      workers[workerId] = {
        state,
        messageIds,
        lastPokeAt: Date.now(),
      };
    } else {
      workers[workerId] = {
        state,
        messageIds,
      };
    }
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

    scheduleTick();
  }

  function stop(force?: boolean): Promise<void> {
    if (primaryState === "stopped") {
      if (!stoppingPromise) {
        // (This should not happen)
        throw new Error("No promise available, but primaryState is stopped");
      }
      return stoppingPromise;
    } else if (primaryState === "stopping") {
      if (!stoppingPromise) {
        // (This should not happen)
        throw new Error("No promise available, but primaryState is stopping");
      }

      if (force) {
        // We've already asked nicely. Now just stop.
        justStop();
        stoppingPromise = Promise.resolve();
      }

      return stoppingPromise;
    }

    setPrimaryState("stopping");

    stoppingPromise = new Promise((resolve) => {
      emitter.emit("stopping");

      if (force) {
        justStop();
        return;
      }

      sendTracker
        .waitForIdle()
        .then(() => driver.stop())
        .then(() => {
          tryToStop();
        });

      function tryToStop() {
        const workersLeft = Object.keys(workers);

        if (workersLeft.length > 0) {
          stopAllWorkers();
          setTimeout(tryToStop, 100);
          return;
        }

        justStop();
        resolve();
      }
    });

    return stoppingPromise;

    function stopAllWorkers() {
      const workersLeft = Object.keys(workers);
      workersLeft.forEach((id) => {
        delete workers[id];
        driver.takeWorkerOffline(id, !!force);
      });
    }

    function justStop() {
      stopAllWorkers();

      setPrimaryState("stopped");

      stopTicking();

      emitter.removeAllListeners();
    }
  }

  /**
   * tick() is called _often_ to ensure new work is queued up and incoming
   * messages have been processed. It is set up so that only one tick() can
   * be executing at a time. If it returns a Promise, no ticking will be
   * allowed until that Promise has completed.
   */
  function tick(): Promise<unknown> | void {
    if (primaryState === "not_started") {
      start();
      return;
    }

    // First, handle any system messages. These be handled synchronously and
    // are prioritized over user messages. They tell us things
    // like when new workers come online or go offline, when they're too
    // busy, etc.

    for (let i = 0; i < messagesToProcess.length; i++) {
      const envelope = messagesToProcess[i];

      if (envelope.message.type === "message") {
        continue;
      }

      processSystemMessage(envelope);

      messagesToProcess.splice(i, 1);
      i--;
    }

    // Next, send messages we've got queued up to our workers.
    // (We only send messages while running--when stopping we have to stop
    // feeding our workers more work to do)
    if (primaryState === "started") {
      const readyWorkerIds = getReadyWorkerIds();

      if (readyWorkerIds.length === 0) {
        // We don't have any workers ready.
        adjustWorkers();
        scheduleTick("no workers ready", 500);
      } else {
        for (let i = 0; i < messagesToSend.length; i++) {
          const envelope = messagesToSend[i];
          const workerId =
            envelope.toWorkerId ??
            readyWorkerIds[Math.floor(Math.random() * readyWorkerIds.length)];

          const sent = sendMessageToWorker(envelope, workerId);

          if (sent) {
            messagesToSend.splice(i, 1);
            break;
          }
        }

        if (messagesToSend.length > 0) {
          scheduleTick("more to send");
        }
      }
    }

    // Finally, execute 1 user message at a time
    for (let i = 0; i < messagesToProcess.length; i++) {
      const envelope = messagesToProcess[i];
      const { message } = envelope;

      if (message.type !== "message") {
        continue;
      }

      messagesToProcess.splice(i, 1);

      emitter.emit("receive", message.message);

      // We process a single user message each tick, since they can be
      // asynchronous.
      return executeHandlers(message.message);
    }
  }

  function sendMessageToWorker(
    envelope: WorkerControlMessageEnvelope,
    workerId: string
  ): boolean {
    const { message } = envelope;

    const worker = workers[workerId];

    if (!worker) {
      log(
        `Can't send ${message.id} to worker ${workerId} (does not exist). Ignoring.`
      );
      return true; // We lie and say we sent it to force message's removal
    }

    if (worker.state === "initializing") {
      // This _should not_ happen
      throw new Error(`Worker ${workerId} is still initializing`);
    }

    log.enabled &&
      log(`send to ${workerId} (${worker.state}): ${JSON.stringify(message)}`);

    worker.messageIds.push(message.id);

    sendTracker.sent(envelope);

    if (message.type === "message") {
      emitter.emit("send", message.message);
    }

    // sendToWorker() is asynchronous, but we don't let it block future processing.
    driver.sendToWorker(workerId, message).catch((err) => {
      // Send failed, so re-enqueue the message to be sent to another worker.
      sendTracker.errorSending(envelope);
      messagesToSend.push(envelope);

      const worker = workers[workerId];
      if (worker == null) {
        log(
          `[${workerId}] Error sending message to worker (no longer exists)`,
          err
        );
      } else {
        log(`[${workerId}] Error sending message to worker`, err);
        worker.messageIds = worker.messageIds.filter((id) => id !== message.id);
      }

      scheduleTick("error during send to worker");
      return;
    });

    // Record when we last attempted to ask a "busy" worker if it was really
    // still busy.
    if (worker.state === "busy") {
      worker.lastPokeAt = Date.now();
    }

    return true;
  }

  function getReadyWorkerIds(): string[] {
    const allWorkerIds = Object.keys(workers);

    return allWorkerIds.filter((id) => {
      const worker = workers[id];

      switch (worker.state) {
        case "initializing":
          // No sending to initializing workers
          return false;

        case "ready":
          return true;

        case "busy":
          // We can periodically send messages to busy workers to see if they're
          // actually still busy.
          const timeSinceLastPoke = Date.now() - worker.lastPokeAt;
          return timeSinceLastPoke >= BUSY_POKE_INTERVAL;
      }
    });
  }
}
