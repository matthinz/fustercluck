import child from "child_process";
import cluster, { Worker as ClusterWorker } from "cluster";
import debug from "debug";
import os from "os";
import { ControlMessage, parseControlMessage, parseEnvelope } from "./messages";

import { Envelope, Primary, RunOptions } from "./types";

type PrimaryControl<WorkerMessage> = {
  sendToWorkers(messages: WorkerMessage[] | WorkerMessage): Promise<void>;
  stop(): Promise<void>;
};

type WorkerState = "initializing" | "ready" | "busy";

type PrimaryState =
  | "initializing"
  | "running"
  | "interrupted"
  | "processing"
  | "dispatching";

export function runPrimary<
  PrimaryMessage extends child.Serializable,
  WorkerMessage extends child.Serializable
>(
  options: RunOptions<PrimaryMessage, WorkerMessage>
): PrimaryControl<WorkerMessage> {
  /**
   * Batches are groups of messages we monitor to see when they're
   * taken up / processed by a worker.
   */
  type MessageBatch = {
    resolve: () => void;
    reject: (err: any) => void;
    envelopes: Envelope<WorkerMessage>[];
  };

  const log = debug(`fustercluck:primary`);

  // `primaryState` tracks the current state of the primary.
  let primaryState: PrimaryState = "initializing";

  let primary: Primary<PrimaryMessage, WorkerMessage> | undefined;

  // `workerStates` is a dictionary linking worker id to the associated state.
  const workerStates: { [id: number]: WorkerState } = {};

  // `tickImmediate` holds on to the handle of the timer used to schedule the
  // next `tick` invocation to duplicate scheduling.
  let tickImmediate: NodeJS.Immediate | undefined;

  // These queues are first-in-first-out.
  const controlMessagesToProcess: {
    workerId: number;
    envelope: Envelope<ControlMessage<WorkerMessage>>;
  }[] = [];

  const controlMessagesToSend: {
    workerId: number;
    envelope: Envelope<ControlMessage<WorkerMessage>>;
  }[] = [];

  const messagesToProcess: {
    workerId?: number;
    envelope: Envelope<PrimaryMessage>;
  }[] = [];

  const messagesToSend: Envelope<WorkerMessage>[] = [];

  let receivedMessageBatches: MessageBatch[] = [];

  let processedMessageBatches: MessageBatch[] = [];

  let nextMessageId = 0;

  Promise.resolve(options.createPrimary()).then((createdPrimary) => {
    if (primaryState === "interrupted") {
      return;
    }

    setPrimaryState("running");

    primary = createdPrimary;

    bindEvents();

    spawnWorkers();
  });

  return {
    stop,
    sendToWorkers,
  };

  function bindEvents() {
    cluster.on("disconnect", handleDisconnect);
    cluster.on("online", handleOnline);
    cluster.on("message", handleMessage);
    process.on("SIGINT", handleSigInt);
  }

  function envelope<Message>(message: Message): Envelope<Message> {
    return {
      id: nextMessageId++,
      message,
    };
  }

  function handleDisconnect(worker: ClusterWorker) {
    log(`[${worker.id}] disconnect`);

    delete workerStates[worker.id];
    spawnWorkers();
  }

  function handleMessage(worker: ClusterWorker, input: unknown) {
    if (!primary) {
      throw new Error("primary not available");
    }

    const controlMessageEnvelope = parseEnvelope<ControlMessage<WorkerMessage>>(
      input,
      parseControlMessage
    );

    if (controlMessageEnvelope != null) {
      controlMessagesToProcess.push({
        workerId: worker.id,
        envelope: controlMessageEnvelope,
      });
      scheduleTick();
      return;
    }

    const envelope = parseEnvelope(input, primary.parseMessage);

    if (envelope) {
      messagesToProcess.push({
        workerId: worker.id,
        envelope: envelope,
      });
      scheduleTick();
      return;
    }

    // This is not a message we can process.
    log("Received unprocessable message", input);
  }

  /**
   * Handles a new worker coming online. Performs any initialization required,
   * then marks the worker as "ready" to be sent messages.
   */
  function handleOnline(worker: ClusterWorker) {
    if (!primary) {
      throw new Error("primary not available");
    }

    if (primaryState === "interrupted") {
      worker.disconnect();
      return;
    }

    const { initializeWorker } = primary;
    const { id } = worker;

    log(`[${id}] online`);

    if (!initializeWorker) {
      // No initialization is necessary
      workerStates[id] = "ready";
      return;
    }

    workerStates[worker.id] = "initializing";

    // We use a custom version of sendToWorker() here that
    // tracks the messages that get send at lets us wait
    // for them to be confirmed.

    const initMessagesToSend: WorkerMessage[] = [];

    const sendToWorkerForInit = (m: WorkerMessage) => {
      initMessagesToSend.push(m);
    };

    initializeWorker
      .call(primary, {
        sendToWorker: sendToWorkerForInit,
      })
      .then(() => sendToWorkersAndWaitForProcessing(initMessagesToSend))
      .catch((err) => {
        log(`[${id}]: Error during initialization`, err);
        worker.disconnect();
        return false;
      })
      .then((succeeded) => {
        if (primaryState === "interrupted") {
          worker.disconnect();
          return;
        }

        if (succeeded && worker.isConnected()) {
          workerStates[worker.id] = "ready";
          scheduleTick();
        }
      });
  }

  function handleSigInt() {
    if (primaryState === "interrupted") {
      // We were already interrupted once, we're being asked _again_ to close.
      process.exit(1);
      return;
    }

    stop().then(() => {
      process.exit();
    });
  }

  function handleWorkerProcessed(messageIds: number[]) {
    processedMessageBatches = processedMessageBatches
      .map((batch) => {
        batch.envelopes = batch.envelopes
          .map((e) => {
            if (messageIds.includes(e.id)) {
              return undefined;
            }
            return e;
          })
          .filter((e) => e !== undefined) as Envelope<WorkerMessage>[];

        if (batch.envelopes.length === 0) {
          setImmediate(batch.resolve);
          return undefined;
        }

        return batch;
      })
      .filter((b) => b !== undefined) as MessageBatch[];
  }

  function handleWorkerReceived(messageIds: number[]) {
    receivedMessageBatches = receivedMessageBatches
      .map((batch) => {
        batch.envelopes = batch.envelopes
          .map((e) => {
            if (messageIds.includes(e.id)) {
              return undefined;
            }
            return e;
          })
          .filter((e) => e !== undefined) as Envelope<WorkerMessage>[];

        if (batch.envelopes.length === 0) {
          setImmediate(batch.resolve);
          return undefined;
        }

        return batch;
      })
      .filter((b) => b !== undefined) as MessageBatch[];
  }

  function processControlMessage({
    workerId,
    envelope: { message },
  }: {
    workerId: number;
    envelope: Envelope<ControlMessage<WorkerMessage>>;
  }) {
    log.enabled && log(`receive from ${workerId}: ${JSON.stringify(message)}`);

    switch (message.__type__) {
      case "worker_busy": {
        // This worker is too busy to process the things we sent it.
        setWorkerState(workerId, "busy");
        messagesToSend.push(...message.envelopes);
        scheduleTick();
        break;
      }
      case "worker_processed": {
        handleWorkerProcessed(message.messageIds);
        setWorkerState(workerId, message.canTakeMore ? "ready" : "busy");
        scheduleTick();
        break;
      }
      case "worker_ready": {
        setWorkerState(workerId, "ready");
        scheduleTick();
        break;
      }
      case "worker_received": {
        // This worker is taking on some of the messages we've sent it!
        handleWorkerReceived(message.messageIds);
        setWorkerState(workerId, message.canTakeMore ? "ready" : "busy");
        scheduleTick();
        break;
      }
    }
  }

  function processMessage(m: PrimaryMessage): Promise<unknown> {
    if (!primary) {
      throw new Error("primary not available");
    }

    let promise: Promise<unknown>;

    try {
      promise = Promise.resolve(
        primary.handle(m, { sendToPrimary, sendToWorker })
      );
    } catch (err: any) {
      promise = Promise.reject(err);
    }

    return promise.catch((err) => {
      log("Error processing message for primary");
      log(m);
      log(err);
      throw err;
    });
  }

  function scheduleTick() {
    if (!tickImmediate) {
      setImmediate(tick);
    }
  }

  function sendToPrimary(message: PrimaryMessage) {
    messagesToProcess.push({
      envelope: envelope(message),
    });
    scheduleTick();
  }

  function sendToWorker(message: WorkerMessage) {
    messagesToSend.push(envelope(message));
    scheduleTick();
  }

  /**
   * Sends a batch of messages to workers and returns a Promise that resolves
   * when _all_ messages have been received by a worker.
   * @param messages
   */
  function sendToWorkers(
    messages: WorkerMessage[] | WorkerMessage
  ): Promise<void> {
    return new Promise((resolve, reject) => {
      if (primaryState === "interrupted") {
        reject(
          new Error("Can't send to workers after stop() has been called.")
        );
        return;
      }

      const envelopes = (Array.isArray(messages) ? messages : [messages]).map(
        envelope
      );

      if (envelopes.length === 0) {
        resolve();
        return;
      }

      messagesToSend.push(...envelopes);

      receivedMessageBatches.push({
        envelopes,
        reject,
        resolve,
      });

      scheduleTick();
    });
  }

  function sendToWorkersAndWaitForProcessing(
    messages: WorkerMessage[] | WorkerMessage
  ): Promise<void> {
    return new Promise((resolve, reject) => {
      if (primaryState === "interrupted") {
        reject(
          new Error("Can't send to workers after stop() has been called.")
        );
      }

      const envelopes = (Array.isArray(messages) ? messages : [messages]).map(
        envelope
      );

      if (envelopes.length === 0) {
        resolve();
        return;
      }

      messagesToSend.push(...envelopes);

      processedMessageBatches.push({
        envelopes,
        reject,
        resolve,
      });

      scheduleTick();
    });
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

  function stop(): Promise<void> {
    return new Promise((resolve, reject) => {
      setPrimaryState("interrupted");

      tryToStop();

      function tryToStop() {
        const canStop = Object.keys(workerStates).length === 0;

        if (!canStop) {
          setTimeout(tryToStop, 100);
          return;
        }

        process.off("message", handleMessage);
        cluster.off("online", handleOnline);
        cluster.off("disconnect", handleDisconnect);

        if (tickImmediate) {
          clearImmediate(tickImmediate);
          tickImmediate = undefined;
        }
      }
    });
  }

  /**
   * tick() is called _often_ to ensure new work is queued up and incoming
   * messages have been processed.
   */
  function tick() {
    tickImmediate = undefined;

    // Send any control messages we need to
    if (primaryState !== "interrupted") {
      while (controlMessagesToSend.length > 0) {
        const m = controlMessagesToSend.shift();
        if (!m) {
          throw new Error("null entry in controlMessagesToSend");
        }

        // control messages have to be sent to a specific worker
        const worker = cluster.workers && cluster.workers[m.workerId];
        if (!worker) {
          log(
            `[${
              m.workerId
            }] Attempting to send control message to nonexisting worker: ${JSON.stringify(
              m
            )}`
          );
          continue;
        }

        worker.send(m, (err) => {
          if (err) {
            log(`[${m.workerId}] Error sending message`, err);
            controlMessagesToSend.push(m);
            scheduleTick();
          }
        });
      }
    }

    // Process any control messages we've received.
    // (We always handle control messages, even if we're shutting down.)
    if (controlMessagesToProcess.length > 0) {
      const m = controlMessagesToProcess.shift();
      if (!m) {
        throw new Error("null entry in controlMessagesToProcess");
      }
      processControlMessage(m);
      scheduleTick();
      return;
    }

    // Then, process any messages we've received, one at a time.
    if (primaryState !== "processing" && messagesToProcess.length > 0) {
      const m = messagesToProcess.shift();
      if (!m) {
        throw new Error("null entry in messagesToProcess");
      }
      const prevPrimaryState = primaryState;
      setPrimaryState("processing");
      processMessage(m.envelope.message).then(() => {
        if (primaryState === "processing") {
          setPrimaryState(prevPrimaryState);
        }
        scheduleTick();
      });
      return;
    }

    if (primaryState === "interrupted") {
      // We've been interrupted, so we're not sending any more messages
      // to our workers.
      return;
    }

    // Finally, send messages we've got queued up to our workers.
    if (messagesToSend.length > 0) {
      let readyWorkers = Object.keys(workerStates).filter(
        (id) => workerStates[Number(id)] === "ready"
      );

      if (readyWorkers.length === 0) {
        // We don't have any workers we can send to.
        // So let's send to all of them and let them tell us whether they
        // are still busy.
        readyWorkers = Object.keys(workerStates);
      }

      if (readyWorkers.length === 0) {
        // We dont' have any workers to actually do any work
        scheduleTick();
        return;
      }

      while (messagesToSend.length > 0) {
        const envelope = messagesToSend.shift();
        if (!envelope) {
          throw new Error("null entry in messagesToSend");
        }

        const workerId =
          readyWorkers[Math.floor(Math.random() * readyWorkers.length)];
        const worker = cluster.workers && cluster.workers[workerId];

        if (!worker) {
          // We think we have a worker that is not actually online. This implies
          // that we've gone out-of-date w/r/t Node's cluster. So quietly put
          // the message back on the queue and start over
          log(`[${workerId}] worker id does not exist`);
          messagesToSend.unshift(envelope);
          scheduleTick();
          return;
        }

        log(`send to ${workerId}: ${JSON.stringify(envelope)}`);

        worker.send(envelope, (err) => {
          if (err) {
            // An error during send probably means a worker died?
            log(`[${workerId}] Error sending message to worker`, err);
            messagesToSend.push(envelope);
            scheduleTick();
            return;
          }
        });
      }
    }
  }

  function workersWanted(): number {
    if (options.workerCount == null) {
      return os.cpus().length;
    }

    if (typeof options.workerCount === "function") {
      return options.workerCount();
    }

    return options.workerCount;
  }
}
