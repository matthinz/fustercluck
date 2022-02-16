import child from "child_process";
import { Driver } from "./driver";

export type MessageBase = {
  type: string;
} & child.Serializable;

/**
 * A map of message types to sets of functions registered to handle messages of that type.
 */
export type MessageHandlers<Message extends MessageBase> = {
  [MessageType in Message["type"] as string]: ((
    m: Message & { type: MessageType }
  ) => void | Promise<void>)[];
};

type WorkerMessageHandler<PrimaryMessage, WorkerMessage> = (
  m: WorkerMessage
) => void | Promise<void>;

type PrimaryMessageHandler<PrimaryMessage, WorkerMessage> = (
  m: PrimaryMessage
) => void | Promise<void>;

export type WorkerError = {
  code?: string;
  message: string;
  stack?: string;
};

export type Primary<
  PrimaryMessage extends MessageBase,
  WorkerMessage extends MessageBase
> = {
  role: "primary";

  /**
   * Adds a handler for messages of the given type.
   * @param type Message type to handle.
   * @param handler Handler function. This function may return a Promise.
   */
  handle<MessageType extends PrimaryMessage["type"]>(
    messageType: MessageType,
    handler: PrimaryMessageHandler<
      PrimaryMessage & { type: MessageType },
      WorkerMessage
    >
  ): void;

  /**
   * Specifies how to initialize workers.
   * @param initializer Either a message to send, or a function that returns a message to send.
   */
  initializeWorkersWith(
    initializer:
      | WorkerMessage
      | (() => WorkerMessage)
      | (() => Promise<WorkerMessage>)
  ): void;

  /**
   * Adds a handler for the "error" event, which fires whenever an error
   * is encountered while a worker is handling a message.
   * @param eventName
   * @param handler
   */
  on(eventName: "error", handler: (err: WorkerError) => void): void;

  /**
   * Adds a handler for the "receive" event, which fires when the primary
   * receives a new message.
   * @param eventName
   * @param handler
   */
  on(eventName: "receive", handler: (m: PrimaryMessage) => void): void;

  /**
   * Adds a handler for the "send" event, which fires when the primary sends
   * a message to a worker.
   * @param eventName
   * @param handler
   */
  on(eventName: "send", handler: (m: WorkerMessage) => void): void;

  /**
   * Adds a handler for the "stop" event, which fires when the primary is
   * stopping its processing.
   * @param eventName
   * @param handler
   */
  on(eventName: "stop", handler: () => void): void;

  /**
   * Puts one or more on the processing queue for the Primary.
   * @param messages
   */
  sendToPrimary(messages: PrimaryMessage | PrimaryMessage[]): void;

  /**
   * Dispatches one or more messages to workers.
   * @param messages
   */
  sendToWorkers(messages: WorkerMessage | WorkerMessage[]): void;

  /**
   * Dispatches one or more messages to workers.
   * @param messages
   * @returns A Promise that resolves once all messages have been _received_ by a worker.
   */
  sendToWorkersAndWaitForReceipt(
    messages: WorkerMessage | WorkerMessage[]
  ): Promise<void>;

  /**
   * Dispatches one or more messages to workers.
   * @param messages
   * @returns A Promise that resolves once all messages have been _processed_ by a worker.
   */
  sendToWorkersAndWaitForProcessing(
    messages: WorkerMessage | WorkerMessage[]
  ): Promise<void>;

  /**
   * Stops processing. If force is `true`, any running workers will be forcibly
   * disconnected. Otherwise `stop` will wait for any messages currently being
   * processed to complete before disconnecting them.
   * @returns A Promise that resolves once all workers have been disconnected.
   */
  stop(force?: boolean): Promise<void>;
};

export type Worker<
  PrimaryMessage extends MessageBase,
  WorkerMessage extends MessageBase
> = {
  role: "worker";

  /**
   * @param check A function that returns whether this worker is currently "busy".
   */
  addBusyCheck(check: () => boolean): void;

  /**
   * Adds a handler for messages of the given type.
   * @param type Message type to handle.
   * @param handler Handler function. This function may return a Promise.
   */
  handle<MessageType extends WorkerMessage["type"]>(
    messageType: MessageType,
    handler: WorkerMessageHandler<
      PrimaryMessage,
      WorkerMessage & { type: MessageType }
    >
  ): void;

  /**
   * Sends one or more messages to the primary.
   * @param messages
   */
  sendToPrimary(messages: PrimaryMessage | PrimaryMessage[]): void;
};

export type StartOptions<
  PrimaryMessage extends MessageBase,
  WorkerMessage extends MessageBase
> = {
  /**
   * Custom driver to use. Defaults to the Node.js cluster driver.
   */
  driver?: Driver;

  /**
   * Function used to attempt to parse unknown input into a strongly-typed message for the Primary.
   */
  parsePrimaryMessage?: (m: unknown) => PrimaryMessage | undefined;

  /**
   * Function used to attempt to parse unknown input into a strongly-typed message for the Worker.
   */
  parseWorkerMessage?: (m: unknown) => WorkerMessage | undefined;
};

export type StartResult<
  PrimaryMessage extends MessageBase,
  WorkerMessage extends MessageBase
> =
  | Primary<PrimaryMessage, WorkerMessage>
  | Worker<PrimaryMessage, WorkerMessage>;
