export type ExposedPromise<T> = {
  readonly promise: Promise<T>;
  resolve: (result: T) => void;
  reject: (err: any) => void;
};

/**
 * makePromise() constructs a Promise<T> with its resolve/reject mechanism
 * exposed.
 * @returns
 */
export function makePromise<T>(): ExposedPromise<T> {
  let resolveInsidePromise: (value: T) => void | undefined;
  let rejectInsidePromise: (err: any) => void | undefined;

  let onPromiseAvailable:
    | ((resolve: (value: T) => void, reject: (err: any) => void) => void)
    | undefined;

  const promise = new Promise<T>((resolve, reject) => {
    resolveInsidePromise = resolve;
    rejectInsidePromise = reject;
    if (onPromiseAvailable) {
      onPromiseAvailable(resolve, reject);
    }
  });

  return { promise, resolve, reject };

  function resolve(value: T) {
    if (resolveInsidePromise) {
      resolveInsidePromise(value);
    } else if (!onPromiseAvailable) {
      onPromiseAvailable = (resolve) => {
        resolve(value);
      };
    }
  }

  function reject(err: any) {
    if (rejectInsidePromise) {
      rejectInsidePromise(err);
    } else if (!onPromiseAvailable) {
      onPromiseAvailable = (_, reject) => {
        reject(err);
      };
    }
  }
}
