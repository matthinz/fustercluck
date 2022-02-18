import { EventEmitter } from "events";
import { MessageBase, Worker } from "../types";
import { startWorker } from "../worker";
import { Driver, DriverEventName } from "./types";

export function createMockDriver<
  PrimaryMessage extends MessageBase,
  WorkerMessage extends MessageBase
>(
  maxWorkers: number,
  customInitWorker: (worker: Worker<PrimaryMessage, WorkerMessage>) => void
): Driver {
  const workers: { [id: string]: ((message: unknown) => void) | false } = {};
  let currentWorkerId = 0;
  let okToLeakCurrentWorkerId = false;
  const emitter = new EventEmitter();

  const driver = {
    getMaxNumberOfWorkers,
    getWorkerId,
    initWorker,
    on,
    requestNewWorker,
    role,
    sendToPrimary,
    sendToWorker,
    stop,
    takeWorkerOffline,
  };

  return driver;

  function getMaxNumberOfWorkers() {
    return maxWorkers;
  }

  function getWorkerId() {
    if (!okToLeakCurrentWorkerId) {
      throw new Error("Can't leak current worker ID");
    }
    return String(currentWorkerId);
  }

  function initWorker(workerId: string) {}

  function on(eventName: DriverEventName, listener: (...args: any[]) => void) {
    if (eventName === "messageFromPrimary") {
      // Special case: this only gets set for the most recent worker
      if (currentWorkerId === 0) {
        throw new Error("no worker started");
      }
      if (workers[currentWorkerId]) {
        throw new Error(
          `${eventName} listener already registered for worker ${currentWorkerId}`
        );
      }
      workers[currentWorkerId] = listener;
    } else {
      emitter.on(eventName, listener);
    }
  }

  function requestNewWorker() {
    setImmediate(() => {
      currentWorkerId++;
      const id = String(currentWorkerId);
      workers[id] = false;

      okToLeakCurrentWorkerId = true;
      try {
        const worker = startWorker<PrimaryMessage, WorkerMessage>({ driver });
        customInitWorker(worker);
      } finally {
        okToLeakCurrentWorkerId = false;
      }

      emitter.emit("workerOnline", id);
    });
  }

  function role(): "primary" | "worker" {
    throw new Error("Not supported");
  }

  function sendToPrimary(
    fromWorkerId: string,
    message: unknown
  ): Promise<void> {
    return new Promise((resolve) => {
      emitter.emit("messageFromWorker", fromWorkerId, message);
      resolve();
    });
  }

  function sendToWorker(id: string, message: unknown): Promise<void> {
    return new Promise((resolve, reject) => {
      const listener = workers[id];

      if (listener == null) {
        reject(new Error(`Invalid worker: ${id}`));
        return;
      }

      if (listener === false) {
        return;
      }

      setImmediate(() => {
        listener(message);
        setImmediate(resolve);
      });
    });
  }

  function stop(): Promise<void> {
    return new Promise((resolve) => {
      emitter.removeAllListeners();
      resolve();
    });
  }

  function takeWorkerOffline(id: string) {
    if (workers[id] == null) {
      throw new Error(`Invalid worker id: ${id}`);
    }
    delete workers[id];
    emitter.emit("workerOffline", id);
  }
}
