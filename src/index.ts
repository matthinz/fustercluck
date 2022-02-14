import cluster from "cluster";
import { startPrimary } from "./primary";
import { MessageBase, StartOptions, StartResult } from "./types";
import { startWorker } from "./worker";

export { Primary, Worker } from "./types";

export function start<
  PrimaryMessage extends MessageBase,
  WorkerMessage extends MessageBase
>(
  options?: StartOptions<PrimaryMessage, WorkerMessage>
): StartResult<PrimaryMessage, WorkerMessage> {
  if (cluster.isPrimary) {
    return startPrimary(options);
  } else if (cluster.isWorker) {
    return startWorker(options);
  } else {
    throw new Error("Neither a primary nor a worker. Is this Node 16?");
  }
}
