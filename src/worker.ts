import debug from "debug";
import { createClusterDriver } from "./driver";
import { MessageBase, MessageHandlers, StartOptions, Worker } from "./types";
import {
  parseWorkerControlMessage,
  PrimaryControlMessage,
  WorkerControlMessage,
} from "./messages";
import { toDebuggingJson } from "./util";

// Interval used to debounce notifications to the primary about messages we've processed
const NOTIFY_PRIMARY_OF_PROCESSED_DEBOUNCE_INTERVAL = 250;

// Max # of messages that can be in the "we've processed" list
const MAX_PROCESSED_BATCH_SIZE = 1000;

type WorkerState = "not_started" | "started" | "draining";

export function startWorker<
  PrimaryMessage extends MessageBase,
  WorkerMessage extends MessageBase
>(
  options?: StartOptions<PrimaryMessage, WorkerMessage>
): Worker<PrimaryMessage, WorkerMessage> {
  const driver = options?.driver ?? createClusterDriver();

  const workerId = driver.getWorkerId();

  const log = debug(`fustercluck:worker:${workerId}`);

  /**
   * workerState tracks the current state of this worker.
   */
  let workerState: WorkerState = "not_started";

  /**
   * Busy checks are used to evaluate whether this worker is capable of
   * receiving more messages. This array is extended by calling
   * addBusyCheck().
   */
  const busyChecks: (() => boolean)[] = [];

  /**
   * Registered message handlers are called when processing messages of
   * a specific type.
   */
  const messageHandlers = {} as MessageHandlers<WorkerMessage>;

  const messagesToProcess: WorkerControlMessage<WorkerMessage>[] = [];

  const messagesToSend: PrimaryControlMessage<PrimaryMessage>[] = [];

  let processedMessageIds: number[] = [];

  let notifyPrimaryTimeout: NodeJS.Timeout | undefined;

  let tickImmediate: NodeJS.Immediate | undefined;

  let nextMessageId = 0;

  driver.initWorker(workerId);

  return { role: "worker", addBusyCheck, sendToPrimary, handle };

  /**
   * Adds a function to be called whenever the worker needs to check whether
   * it is busy. This function can return `true` meaning "I'm busy" or `false`
   * meaning "I'm not busy".
   * @param check
   */
  function addBusyCheck(check: () => boolean) {
    busyChecks.push(check);
  }

  /**
   * Executes all registered handlers for the given message.
   * @param m
   * @returns {Promise<void>} A Promise that resolves after execution has completed.
   */
  function executeHandlers(m: WorkerMessage): Promise<void> {
    const handlers = messageHandlers[m.type] ?? [];

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

  /**
   * Adds new handlers for specific types of messages.
   * @param type
   * @param handler
   */
  function handle<MessageType extends WorkerMessage["type"]>(
    type: MessageType,
    handler: (m: WorkerMessage & { type: MessageType }) => void | Promise<void>
  ) {
    const list = messageHandlers[type] ?? [];
    list.push(handler);
    messageHandlers[type] = list;

    // NOTE: If we don't have any handlers, there's no point in starting the
    //       worker. So we delay starting until a handler has been added.
    start();
  }

  /**
   * Handles a message received by the worker process.
   * @param m
   * @returns
   */
  function handleMessage(m: unknown) {
    const parsed = parseWorkerControlMessage(m, options?.parseWorkerMessage);

    if (parsed == null) {
      log.enabled && log(`INVALID MESSAGE: ${toDebuggingJson(m)}`);
      return;
    }

    log.enabled &&
      log(`ENQUEUE: ${parsed.id} (${isBusy() ? "busy" : "not busy"})`);

    messagesToProcess.push(parsed);

    scheduleTick();
  }

  /**
   * Handles a request that we shut down.
   */
  function handleShutdown() {
    switch (workerState) {
      case "not_started":
        return;

      case "started":
        workerState = "draining";
        scheduleTick();
        return;

      case "draining":
        process.exit();
        return;
    }
  }

  function isBusy(): boolean {
    return busyChecks.reduce<boolean>((result, check) => {
      return result || check();
    }, false);
  }

  function notifyPrimaryOfProcessedMessages() {
    if (notifyPrimaryTimeout) {
      clearTimeout(notifyPrimaryTimeout);
    }

    if (processedMessageIds.length > MAX_PROCESSED_BATCH_SIZE) {
      doNotification();
    } else {
      notifyPrimaryTimeout = setTimeout(
        doNotification,
        NOTIFY_PRIMARY_OF_PROCESSED_DEBOUNCE_INTERVAL
      );
    }

    function doNotification() {
      if (processedMessageIds.length === 0) {
        return;
      }

      sendToControlMessagesToPrimary([
        {
          id: nextMessageId++,
          type: "worker_processed",
          canTakeMore: !isBusy(),
          messageIds: [...processedMessageIds],
        },
      ]);
      processedMessageIds.splice(0, processedMessageIds.length);
    }
  }

  function processControlMessage(
    m: WorkerControlMessage<WorkerMessage> & {
      type: Exclude<WorkerMessage["type"], "message">;
    }
  ) {}

  function scheduleTick() {
    if (!tickImmediate) {
      tickImmediate = setImmediate(tick);
    }
  }

  /**
   * Starts the worker.
   */
  function start() {
    if (workerState !== "not_started") {
      return;
    }

    log("%s -> %s", workerState, "started");

    driver.on("messageFromPrimary", handleMessage);
    driver.on("shutdown", handleShutdown);
    workerState = "started";
  }

  function sendToControlMessagesToPrimary(
    messages: PrimaryControlMessage<PrimaryMessage>[]
  ) {
    if (messages.length === 0) {
      return;
    }

    messagesToSend.push(...messages);

    scheduleTick();
  }

  function sendToPrimary(messages: PrimaryMessage | PrimaryMessage[]) {
    sendToControlMessagesToPrimary(
      (Array.isArray(messages) ? messages : [messages]).map((message) => ({
        id: nextMessageId++,
        type: "message",
        message,
      }))
    );
  }

  /**
   * tick() is the main message processing loop. It does the sending / receiving
   * of messages for the worker.
   */
  function tick() {
    tickImmediate = undefined;

    // ORDER OF OPERATIONS:
    // 1. Handle any non-user messages
    // 2. Send anything that is queued to send to the primary
    // 3. Handle all user messages

    type M = WorkerControlMessage<WorkerMessage>;

    const [userMessagesToProcess, systemMessagesToProcess] =
      messagesToProcess.reduce<[M[], M[]]>(
        ([user, system], m) => {
          m.type === "message" && user.push(m);
          m.type !== "message" && system.push(m);
          return [user, system];
        },
        [[], []]
      );
    messagesToProcess.splice(0, messagesToProcess.length);

    // Handle any system messages
    systemMessagesToProcess.forEach((m) => processControlMessage);

    // Send everything we can back to the primary
    messagesToSend.forEach((m) => {
      log.enabled && log(`send to primary: ${toDebuggingJson(m)}`);
      driver
        .sendToPrimary(workerId, m)
        .catch((err) => {
          log("Error sending message", err, m);
          messagesToSend.push(m);
        })
        .finally(scheduleTick);
    });
    messagesToSend.splice(0, messagesToSend.length);

    const receivedMessageIds: number[] = [];

    if (workerState !== "started") {
      return;
    }

    // Start working through user messages.
    for (
      let m = userMessagesToProcess.shift();
      m;
      m = userMessagesToProcess.shift()
    ) {
      if (m.type !== "message") {
        throw new Error(`Invalid message type for #${m.id}`);
      }

      if (isBusy()) {
        // We're too busy to deal with this. Place the message back on the
        // queue and stop processing further
        userMessagesToProcess.unshift(m);
        break;
      }

      const controlMessage = m;
      const { id, message } = m;

      receivedMessageIds.push(id);

      log.enabled && log(`BEGIN processing ${id}: ${toDebuggingJson(message)}`);

      executeHandlers(message)
        .then(() => {
          processedMessageIds.push(id);
        })
        .catch((err: any) => {
          log(`ERROR processing ${id}`, err);
          messagesToProcess.push(controlMessage);
          scheduleTick();
        })
        .finally(() => {
          log.enabled &&
            log(`END processing ${id}: ${toDebuggingJson(message)}`);
          notifyPrimaryOfProcessedMessages();
        });
    }

    // Any user messages left are ones we couldn't handle because we were
    // too busy. Tell the primary about these ones.
    if (userMessagesToProcess.length > 0) {
      sendToControlMessagesToPrimary([
        {
          type: "worker_busy",
          id: nextMessageId++,
          messageIds: userMessagesToProcess.map(({ id }) => id),
        },
      ]);
    }

    // Finally, schedule a message send to the primary to tell it about
    // the messages we've received so far.
    if (receivedMessageIds.length > 0) {
      sendToControlMessagesToPrimary([
        {
          type: "worker_received",
          id: nextMessageId++,
          messageIds: receivedMessageIds,
          canTakeMore: !isBusy(),
        },
      ]);
    }
  }
}
