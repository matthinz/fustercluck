type Ticker = {
  scheduleTick(): void;
  scheduleTick(reason: string): void;
  scheduleTick(maxDelay: number): void;
  scheduleTick(reason: string, maxDelay: number): void;
  stopTicking(): void;
};

/**
 *
 * @param tick
 * @returns
 */
export function createTicker(tick: () => void | Promise<unknown>): Ticker {
  let immediate: NodeJS.Immediate | undefined;
  let timeout: NodeJS.Timeout | undefined;
  let nextDelay: number | undefined;
  let isTicking = false;
  let isRunning = true;

  return { scheduleTick, stopTicking };

  function invokeTick() {
    immediate = undefined;
    timeout = undefined;
    nextDelay = undefined;

    if (isTicking || !isRunning) {
      return;
    }
    isTicking = true;

    const result = tick();
    if (result) {
      // We have a Promise -- wait until it's done to accept new ticks
      result.finally(() => {
        isTicking = false;
        scheduleTick();
      });
    } else {
      isTicking = false;
    }
  }

  function scheduleTick(reasonOrDelay?: string | number, delay?: number) {
    delay = typeof reasonOrDelay === "number" ? reasonOrDelay : delay;

    if (!delay) {
      // Schedule without any delay

      if (timeout) {
        // We'll be scheduling an immediate, which obviates the need for this.
        clearTimeout(timeout);
        timeout = undefined;
      }

      if (immediate) {
        // We already have an immediate
        return;
      }

      setImmediate(invokeTick);
      nextDelay = undefined;
      return;
    }

    if (immediate) {
      // We already have an immediate scheduled. Go with that.
      return;
    }

    if (nextDelay != null && delay < nextDelay) {
      // Let our new timeout override the existing one
      if (timeout) {
        clearTimeout(timeout);
        timeout = undefined;
      }
      setTimeout(invokeTick, delay);
      nextDelay = delay;
    }
  }

  function stopTicking() {
    isRunning = false;
    if (immediate) {
      clearImmediate(immediate);
      immediate = undefined;
    }
    if (timeout) {
      clearTimeout(timeout);
      timeout = undefined;
    }
  }
}
