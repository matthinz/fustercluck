import { EventEmitter } from "events";
import child from "child_process";
import cluster, { Worker } from "cluster";
import os from "os";
import { Driver } from "./types";

type WorkerEventName =
  | "messageFromPrimary"
  | "messageFromWorker"
  | "workerOnline"
  | "workerOffline"
  | "shutdown";

/**
 * Creates a new Driver implementation that uses Node.js's built-in cluster
 * functionality.
 */
export function createClusterDriver(): Driver {
  const emitter = new EventEmitter();

  bindEvents();

  return {
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

  function bindEvents() {
    if (cluster.isPrimary) {
      cluster.addListener("online", handleOnline);
      cluster.addListener("disconnect", handleDisconnect);
      cluster.on("message", handleClusterMessage);
    } else if (cluster.isWorker) {
      process.on("message", handleProcessMessage);
    }
  }

  function getMaxNumberOfWorkers() {
    return os.cpus().length;
  }

  function getWorkerId(): string {
    const id = cluster.worker?.id;

    if (id == null) {
      throw new Error("Worker ID is not available.");
    }

    return String(id);
  }

  function handleClusterMessage(worker: Worker, message: unknown) {
    emitter.emit("messageFromWorker", String(worker.id), message);
  }

  function handleDisconnect(worker: Worker) {
    emitter.emit("workerOffline", String(worker.id));
  }

  function handleOnline(worker: Worker) {
    emitter.emit("workerOnline", String(worker.id));
  }

  function handleProcessMessage(message: unknown) {
    emitter.emit("messageFromPrimary", message);
  }

  function initWorker(workerId: string) {
    process.on("SIGINT", () => {
      emitter.emit("shutdown");
      stop();
    });
  }

  function on(eventName: WorkerEventName, listener: (...args: any[]) => void) {
    emitter.on(eventName, listener);
  }

  function requestNewWorker() {
    cluster.setupPrimary({
      // @ts-ignore
      serialization: "advanced",
    });

    cluster.fork();
  }

  function role() {
    if (cluster.isPrimary) {
      return "primary";
    } else if (cluster.isWorker) {
      return "worker";
    } else {
      throw new Error("Neither primary nor worker?");
    }
  }

  function sendToPrimary(
    fromWorkerId: string,
    message: unknown
  ): Promise<void> {
    return new Promise((resolve, reject) => {
      const { send } = process;
      if (!send) {
        reject(new Error("`process.send` is not available!"));
        return;
      }
      send.call(process, message, undefined, undefined, (err) => {
        if (err) {
          reject(err);
          return;
        }
        resolve();
      });
    });
  }

  function sendToWorker(workerId: string, message: unknown): Promise<void> {
    return new Promise((resolve, reject) => {
      const worker = cluster.workers && cluster.workers[workerId];
      if (!worker) {
        reject(new Error(`Invalid worker id: ${workerId}`));
        return;
      }
      worker.send(message as child.Serializable, (err) => {
        if (err) {
          reject(err);
          return;
        }
        resolve();
      });
    });
  }

  async function stop(): Promise<void> {
    if (cluster.isPrimary) {
      cluster.removeListener("online", handleOnline);
      cluster.removeListener("disconnect", handleDisconnect);
      cluster.removeListener("message", handleClusterMessage);
    } else if (cluster.isWorker) {
      process.removeListener("message", handleProcessMessage);
    }

    emitter.removeAllListeners();
  }

  function takeWorkerOffline(workerId: string, force: boolean) {
    const worker = cluster.workers && cluster.workers[workerId];
    if (!worker) {
      throw new Error(`Invalid worker: ${workerId}`);
    }
    if (force) {
      worker.kill();
    } else {
      worker.disconnect();
    }
  }
}
