import cluster from "cluster";
import { ControlMessage, parseControlMessage, parseEnvelope } from "./messages";
import { Envelope, RunOptions, Worker } from "./types";
import debug from "debug";

type WorkerControls = {
  stop: () => Promise<void>;
};

export function runWorker<PrimaryMessage, WorkerMessage>(
  options: RunOptions<PrimaryMessage, WorkerMessage>
): WorkerControls {
  const log = debug(`fustercluck:worker:${cluster.worker?.id}`);

  let worker: Worker<PrimaryMessage, WorkerMessage> | undefined;

  let messagesToProcess: Envelope<WorkerMessage>[] = [];

  const controlMessagesToProcess: Envelope<ControlMessage<WorkerMessage>>[] =
    [];

  const messagesToSend: Envelope<
    PrimaryMessage | ControlMessage<WorkerMessage>
  >[] = [];

  // This array tracks the ids of messages that have been submitted for
  // processing but we haven't told the primary about yet
  let currentProcessingBatch: number[] = [];

  let tickImmediate: NodeJS.Immediate | undefined;

  let nextId = 0;

  Promise.resolve(options.createWorker()).then((createdWorker) => {
    worker = createdWorker;
    bindEvents();
  });

  return { stop };

  function bindEvents() {
    process.on("message", handleMessage);
  }

  function envelope<Message>(message: Message): Envelope<Message> {
    return {
      id: nextId++,
      message,
    };
  }

  function handleMessage(m: unknown) {
    if (worker == null) {
      throw new Error("worker not available");
    }

    const workerEnvelope = parseEnvelope(m, worker.parseMessage);

    if (workerEnvelope != null) {
      messagesToProcess.push(workerEnvelope);
      scheduleTick();
      return;
    }

    const controlEnvelope = parseEnvelope<ControlMessage<WorkerMessage>>(
      m,
      parseControlMessage
    );

    if (controlEnvelope != null) {
      controlMessagesToProcess.push(controlEnvelope);
      scheduleTick();
      return;
    }
  }

  function processControlMessage(m: ControlMessage<WorkerMessage>) {
    switch (m.__type__) {
      case "you_up": {
      }
    }
  }

  function scheduleTick() {
    if (!tickImmediate) {
      tickImmediate = setImmediate(tick);
    }
  }

  function sendToPrimary(m: PrimaryMessage | ControlMessage<WorkerMessage>) {
    messagesToSend.push(envelope(m));
    scheduleTick();
  }

  function stop(): Promise<void> {
    process.off("message", handleMessage);
    if (tickImmediate) {
      clearImmediate(tickImmediate);
      tickImmediate = undefined;
    }
    return Promise.resolve();
  }

  function tick() {
    tickImmediate = undefined;

    if (worker == null) {
      throw new Error("worker not available");
    }

    while (controlMessagesToProcess.length > 0) {
      const c = controlMessagesToProcess.shift();
      if (!c) {
        throw new Error("null entry in controlMessagesToProcess");
      }
      processControlMessage(c.message);
    }

    // Send everything we can back to the primary
    const { send } = process;
    if (!send) {
      throw new Error("process.send is not available");
    }

    while (messagesToSend.length > 0) {
      const e = messagesToSend.shift();
      if (!e) {
        throw new Error("null entry in messages to send");
      }
      send.call(process, e, undefined, undefined, (err) => {
        if (err) {
          log("Error sending message", err, e);
          messagesToSend.push(e);
        }
        scheduleTick();
      });
    }

    const busy = worker.isBusy();
    const e = messagesToProcess.shift();

    if (e == null) {
      // We've run out of messages to process. Tell the primary about the
      // ones we've dealt with so far.
      if (currentProcessingBatch.length > 0) {
        sendToPrimary({
          __type__: "worker_handling",
          messageIds: currentProcessingBatch,
          canTakeMore: !busy,
        });
        currentProcessingBatch = [];
      }
      return;
    }

    if (busy) {
      // We're busy. Return all messages to the primary
      sendToPrimary({
        __type__: "worker_too_busy",
        envelopes: [e, ...messagesToProcess],
      });
      messagesToProcess = [];
      return;
    }

    currentProcessingBatch.push(e.id);

    setImmediate((e) => {
      if (worker == null) {
        throw new Error("worker not available");
      }

      let promise: Promise<unknown>;
      try {
        promise = Promise.resolve(worker.handle(e.message, { sendToPrimary }));
      } catch (err: any) {
        promise = Promise.reject(err);
      }

      promise.catch((err: any) => {
        log("Error processing message", err);
        messagesToProcess.push(e);
        scheduleTick();
      });
    }, e);

    if (messagesToProcess.length === 0 && currentProcessingBatch.length > 0) {
      sendToPrimary({
        __type__: "worker_handling",
        messageIds: currentProcessingBatch,
        canTakeMore: !busy,
      });
      currentProcessingBatch = [];
    }
  }
}
