export type DriverEventName =
  | "messageFromPrimary"
  | "messageFromWorker"
  | "workerOffline"
  | "workerOnline";

/**
 * Driver defines the interface through which we interact with the underlying
 * cluster provider.
 */
export type Driver = {
  getMaxNumberOfWorkers(): number;

  getWorkerId(): string;

  requestNewWorker(): void;

  on(
    eventName: "messageFromPrimary",
    handler: (message: unknown) => void
  ): void;

  on(
    eventName: "messageFromWorker",
    handler: (workerId: string, message: unknown) => void
  ): void;

  on(eventName: "workerOnline", handler: (workerId: string) => void): void;

  on(eventName: "workerOffline", handler: (workerId: string) => void): void;

  role(): "primary" | "worker";

  sendToPrimary(fromWorkerId: string, message: unknown): Promise<void>;

  sendToWorker(workerId: string, message: unknown): Promise<void>;

  stop(): Promise<void>;

  takeWorkerOffline(workerId: string): void;
};
