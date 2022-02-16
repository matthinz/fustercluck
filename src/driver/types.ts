/**
 * Driver defines the interface through which we interact with the underlying
 * cluster provider.
 */
export type Driver = {
  getWorkerId(): string;

  requestNewWorker(): void;

  role(): "primary" | "worker";

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

  sendToPrimary(message: unknown): Promise<void>;

  sendToWorker(id: string, message: unknown): Promise<void>;

  stop(): Promise<void>;

  takeWorkerOffline(workerId: string): void;
};
