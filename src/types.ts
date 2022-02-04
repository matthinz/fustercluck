export type SendToWorker<WorkerMessage> = (m: WorkerMessage) => void;

export type SendToPrimary<PrimaryMessage> = (m: PrimaryMessage) => void;

export type Envelope<Message> = {
  id: number;
  message: Message;
};

export type DispatchOptions<PrimaryMessage, WorkerMessage> = {
  sendToWorker: SendToWorker<WorkerMessage>;
  sendToPrimary: SendToPrimary<PrimaryMessage>;
};

export type PrimaryHandleOptions<PrimaryMessage, WorkerMessage> =
  DispatchOptions<PrimaryMessage, WorkerMessage>;

export type WorkerHandleOptions<PrimaryMessage> = {
  sendToPrimary: SendToPrimary<PrimaryMessage>;
};

export type InitializeWorkerOptions<PrimaryMessage, WorkerMessage> = {
  sendToWorker: SendToWorker<WorkerMessage>;
};

export type Primary<PrimaryMessage, WorkerMessage> = {
  /**
   * Handle is called with a message to be processed.
   * If implementations return a Promise, no additional messages will be
   * handled until the Promise resolves.
   */
  handle(
    m: PrimaryMessage,
    options: PrimaryHandleOptions<PrimaryMessage, WorkerMessage>
  ): void | Promise<unknown>;

  /**
   * If defined, initializeWorker is called when a new worker comes online.
   * No additional messages will be sent to the worker until all messages
   * sent during initialization have been processed.
   */
  initializeWorker?: (
    options: InitializeWorkerOptions<PrimaryMessage, WorkerMessage>
  ) => Promise<unknown> | void;

  /**
   * Converts unknown input into a strongly-typed message.
   * @param m
   */
  parseMessage(m: unknown): PrimaryMessage | undefined;

  /**
   * shouldDispatch provides a synchronous hook that can be used to delay the
   * dispatching of a particular message to a worker.
   * @returns `true` to proceed with dispatching, or a number indicating the milliseconds to delay before processing
   */
  shouldDispatch?: (m: PrimaryMessage) => boolean | number;
};

export type Worker<PrimaryMessage, WorkerMessage> = {
  /**
   * Handle is called to process messages sent to this worker.
   * @param m
   * @param options
   */
  handle(m: WorkerMessage, options: WorkerHandleOptions<PrimaryMessage>): void;

  /**
   * isBusy() indicates whether this worker can accept additional messages.
   * It will be called repeatedly.
   */
  isBusy(): boolean;

  /**
   * Converts unknown input into a strongly-typed message.
   * @param m
   */
  parseMessage(m: unknown): WorkerMessage | undefined;
};

/**
 * These options are passed to run() to start the app.
 */
export type RunOptions<PrimaryMessage, WorkerMessage> = {
  /**
   * Time (in ms) that is allowed to elapse between checks to see if a
   * busy worker is still so.
   */
  busyWorkerCheckupInterval?: number | (() => number);

  /**
   * Factory function used to create the Primary instance.
   */
  createPrimary: () =>
    | Promise<Primary<PrimaryMessage, WorkerMessage>>
    | Primary<PrimaryMessage, WorkerMessage>;

  /**
   * Factory function used to create the Worker instance.
   */
  createWorker: () =>
    | Promise<Worker<PrimaryMessage, WorkerMessage>>
    | Worker<PrimaryMessage, WorkerMessage>;

  /**
   * Number of worker processes to span. Defaults to the number of CPUs on
   * the current machine.
   */
  workerCount?: number | (() => number);
};

export type RunResult<PrimaryMessage, WorkerMessage> = {
  /**
   * Allows the primary to send a message to itself.
   */
  sendToPrimary?: (messages: PrimaryMessage | PrimaryMessage[]) => void;

  /**
   * Sends a batch of messages to workers and returns a Promise that resolves
   * once workers have confirmed receipt of the messages.
   * @param messages
   */
  sendToWorkers?: (messages: WorkerMessage | WorkerMessage[]) => Promise<void>;

  /**
   * Stops the running primary or worker.
   */
  stop(): Promise<void>;
};

export type PrimaryRunResult<PrimaryMessage, WorkerMessage> = RunResult<
  PrimaryMessage,
  WorkerMessage
> & {
  /**
   * Sends the given messages to workers and returns a promise that resolves
   * once the send has been completed.
   *
   * @param messages
   */
  sendToWorkers(messages: WorkerMessage[]): Promise<void>;
};
