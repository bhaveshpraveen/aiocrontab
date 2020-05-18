import asyncio
import functools
import logging
import signal
import time
from concurrent.futures import thread
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, NewType, Optional, TypedDict

from croniter import croniter

# logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s Thread-%(thread)d: %(message)s",
    datefmt="%H:%M:%S",
)


# Types
class TRegisteredTask(TypedDict):
    pattern: str
    func: Callable


TSignal = NewType("TSignal", int)


class Task:
    def __init__(
        self,
        pattern: str,
        func: Callable,
        loop: asyncio.AbstractEventLoop,
        executor: thread.ThreadPoolExecutor,
    ) -> None:
        self.pattern = pattern
        self.func = func
        self.loop = loop
        self.executor = executor
        self._time: Optional[datetime] = None
        self._next_timestamp: Optional[float] = None
        self._next_loop_timestamp: Optional[float] = None
        self._running_task: Optional[asyncio.TimerHandle] = None

    @property
    def time(self) -> datetime:
        if self._time is None:
            self._time = datetime.now(timezone.utc)
        return self._time

    @property
    def next_timestamp(self) -> float:
        if self._next_timestamp is None:
            now: datetime = self.time
            iter = croniter(self.pattern, now)
            future_timestamp: float = iter.get_next(ret_type=float)
            self._next_timestamp = future_timestamp

        return self._next_timestamp

    @property
    def next_loop_timestamp(self) -> float:
        """To calculate the timestamp of the next occurence of the specified cron format in context with loop.time().
        Returns
            float: timestamp till the occurence of the self.pattern in context with loop.time()
        """
        if self._next_loop_timestamp is None:
            now_timestamp = self.time.timestamp()
            future_timestamp = self.next_timestamp
            self._next_loop_timestamp = self.loop.time() + (
                future_timestamp - now_timestamp
            )
        return self._next_loop_timestamp

    async def sleep_until_task_completion(self, sleep_time: float) -> None:
        logging.info(f"Non-Block Sleeping for {sleep_time}")
        await asyncio.sleep(sleep_time)
        logging.info(f"Non-Block Sleeping finished for {sleep_time}")

    async def complete_task_lifecycle(self) -> None:
        now: float = self.time.timestamp()
        future_timestamp: float = self.next_timestamp
        await self.schedule()
        await self.sleep_until_task_completion(
            sleep_time=future_timestamp - now
        )

    async def schedule(self) -> None:
        future_loop_timestamp: float = self.next_loop_timestamp
        logging.info(
            f"Task scheduled to be called at {self.next_loop_timestamp}"
        )
        self.loop.call_at(future_loop_timestamp, self.run)

    def run(self) -> None:
        logging.info(f"Scheduling task in Thread")
        self.loop.run_in_executor(self.executor, self.func)


async def handle_cronjob(
    pattern: str,
    func: Callable,
    loop: asyncio.AbstractEventLoop,
    executor: thread.ThreadPoolExecutor,
):
    while True:
        task = Task(pattern=pattern, func=func, loop=loop, executor=executor)
        await task.complete_task_lifecycle()


def func(id: int) -> None:
    logging.info(f"[Task: {id}] Block Sleeping for 300 secs")
    time.sleep(300)
    logging.info(f"[Task: {id}] Block Sleeping Finished for 300 secs.")


# specifies the default error signals to handle/intercept for graceful shutdown
_DEFAULT_ERROR_SIGNALS = [signal.SIGINT]


class Crontab:
    def __init__(
        self,
        loop: Optional[asyncio.AbstractEventLoop] = None,
        executor: Optional[thread.ThreadPoolExecutor] = None,
        error_signals_to_intercept: Optional[List[TSignal]] = None,
    ):
        self.registered_tasks: List[TRegisteredTask] = []
        self._loop = loop
        self._executor = executor
        self.error_signals_to_intercept = (
            error_signals_to_intercept or _DEFAULT_ERROR_SIGNALS
        )

    @property
    def loop(self) -> asyncio.AbstractEventLoop:
        if self._loop is None:
            self._loop = asyncio.get_event_loop()
            self.initialize_event_loop()
        # elif self._loop.is_closed():
        #     self._loop = asyncio.new_event_loop()
        #     asyncio.set_event_loop(self._loop)
        #     self.initialize_event_loop()
        return self._loop

    @property
    def executor(self) -> thread.ThreadPoolExecutor:
        if self._executor is None:
            self._executor = thread.ThreadPoolExecutor()
        return self._executor

    def register(self, pattern: str) -> Callable:
        def decorator(f: Callable) -> Callable:
            task_to_register = TRegisteredTask(pattern=pattern, func=f)
            self.registered_tasks.append(task_to_register)
            return f

        return decorator

    async def shutdown(self, signal: Optional[TSignal] = None) -> None:
        if signal:
            logging.info("Received exit signal %s", signal)
        tasks = [
            t for t in asyncio.all_tasks() if t is not asyncio.current_task()
        ]
        for task in tasks:
            task.cancel()

        logging.info("Cancelling tasks...")
        await asyncio.gather(*tasks, return_exceptions=True)

        # waits until all the background tasks running in different threads to complete.
        logging.info("Waiting for background threads to finish..")
        self.executor.shutdown(wait=True)
        self.loop.stop()

    def handle_error_signals(
        self,
        loop: asyncio.AbstractEventLoop,
        error_signals: Optional[List[TSignal]] = None,
    ) -> None:
        error_signals_to_handle = error_signals or _DEFAULT_ERROR_SIGNALS
        for _signal in error_signals_to_handle:
            loop.add_signal_handler(
                _signal, lambda s=_signal: loop.create_task(self.shutdown(s))
            )

    def handle_exception(
        self, loop: asyncio.AbstractEventLoop, context: Dict[str, Any]
    ) -> None:
        """Set handler as the new event loop exception handler.
        :param loop: Event loop
        :param context: Contains the details of the exception. Details about the context dict: https://docs.python.org/3/library/asyncio-eventloop.html#asyncio.loop.call_exception_handler
        :return: None
        """
        msg = context.get("exception", context["message"])
        logging.error("%s", msg)
        # logging.info("Shutting down..")
        # loop.create_task(self.shutdown())

    def run(self) -> None:
        try:
            for registered_task in self.registered_tasks:
                pattern, func = (
                    registered_task["pattern"],
                    registered_task["func"],
                )
                self.loop.create_task(
                    (
                        handle_cronjob(
                            pattern=pattern,
                            func=func,
                            loop=self.loop,
                            executor=self.executor,
                        )
                    )
                )
            self.loop.run_forever()
        finally:
            logging.info("Closing the loop.")
            self.loop.close()
            logging.info("Shutdown Aiocrontab successfully.")


def main():
    # loop = asyncio.get_event_loop()
    # executor = thread.ThreadPoolExecutor()
    #
    f1 = functools.partial(func, 1)
    f2 = functools.partial(func, 2)
    f3 = functools.partial(func, 3)
    #
    # loop.create_task(handle_cronjob("* * * * *", f1, loop, executor))
    # loop.create_task(handle_cronjob("*/2 * * * *", f2, loop, executor))
    #
    # loop.run_forever()
    #
    # print(f"{time.ctime()}: Completed running.")
    #
    # executor.shutdown(wait=True)

    import aiocrontab

    aiocrontab.register("*/3 * * * *")(f1)
    aiocrontab.register("*/5 * * * *")(f2)
    aiocrontab.run()

    cron = Crontab()
    cron.register("* * * * *")(f3)
    cron.run()


if __name__ == "__main__":
    main()
