import asyncio
import functools
import logging
import signal
import time

from concurrent.futures import thread
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from typing import Any
from typing import Callable
from typing import Dict
from typing import List
from typing import NewType
from typing import Optional
from typing import Tuple
from typing import TypedDict

from croniter import croniter


# logger
logging.Formatter.converter = lambda x, timestamp: datetime.now(
    timezone.utc
).timetuple()
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
        # extra buffer time(seconds) to specify the delay between scheduled tasks.
        self.buffer_time = 30
        self._running_task: Optional[asyncio.TimerHandle] = None
        self.tz = timezone.utc

    def get_next(self) -> Tuple[datetime, datetime]:
        now: datetime = datetime.now(self.tz)
        iter = croniter(self.pattern, now)
        task_datetime: datetime = iter.get_next(datetime)
        return task_datetime, now

    async def sleep_until_task_completion(self, till: datetime) -> None:
        sleep_till_dt: datetime = till + timedelta(seconds=self.buffer_time)
        sleep_till_timestamp: float = sleep_till_dt.timestamp()
        logging.info(f"Non-Block Sleeping for {sleep_till_timestamp}")
        await asyncio.sleep(sleep_till_timestamp)
        logging.info(f"Non-Block Sleeping finished for {sleep_till_timestamp}")

    async def complete_task_lifecycle(self) -> None:
        next_task_datetime, now = self.get_next()
        self.schedule(at=next_task_datetime, now=now)
        await self.sleep_until_task_completion(till=next_task_datetime)

    def schedule(self, at: datetime, now: datetime) -> None:
        next_timestamp: float = at.timestamp()
        now_timestamp: float = now.timestamp()
        next_loop_timestamp: float = self.loop.time() + next_timestamp - now_timestamp
        logging.info(f"Task scheduled to be called at {next_loop_timestamp}")
        self.loop.call_at(next_loop_timestamp, self.run)

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
    logging.info(f"[Task: {id}] Block Sleeping for 15 secs")
    time.sleep(15)
    logging.info(f"[Task: {id}] Block Sleeping Finished for 15 secs.")


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

    def initialize_event_loop(self) -> None:
        self.loop.set_exception_handler(self.handle_exception)
        self.handle_error_signals(self.loop)

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

    aiocrontab.register("* * * * *")(f1)
    aiocrontab.register("* * * * *")(f2)
    aiocrontab.register("* * * * *")(f3)
    aiocrontab.run()

    # cron = Crontab()
    # cron.register("* * * * *")(f3)
    # cron.run()


if __name__ == "__main__":
    main()
