import asyncio
import functools
import time
from concurrent.futures import thread
from datetime import datetime, timezone
from typing import Callable, Optional

from croniter import croniter


def crontab(pattern: str):
    def wrapper(func: Callable):
        def inner_wrapper(*args, **kwargs):
            return func(*args, **kwargs)

        return inner_wrapper

    return wrapper


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
            now: float = self.time
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
                now_timestamp - future_timestamp
            )
        return self._next_loop_timestamp

    async def sleep_until_task_completion(self, sleep_time: float) -> None:
        print(f"{time.ctime()}: Non-Block Sleeping for {sleep_time}")
        await asyncio.sleep(sleep_time)
        print(f"{time.ctime()}: Non-Block Sleeping finished for {sleep_time}")

    async def complete_task_lifecycle(self) -> None:
        now: float = self.time.timestamp()
        future_timestamp: float = self.next_timestamp
        await self.schedule()
        await self.sleep_until_task_completion(
            sleep_time=future_timestamp - now
        )

    async def schedule(self) -> None:
        future_loop_timestamp: float = self.next_loop_timestamp
        print(f"{time.ctime()}: Task scheduled")
        self.loop.call_at(future_loop_timestamp, self.run)

    def run(self) -> None:
        print(f"{time.ctime()}: Scheduling task in Thread")
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
    print(f"{time.ctime()}:[Task: {id}] Block Sleeping for 300 secs")
    time.sleep(300)
    print(
        f"{time.ctime()}: [Task: {id}] Block Sleeping Finished for 300 secs."
    )


def main():
    loop = asyncio.get_event_loop()
    executor = thread.ThreadPoolExecutor()

    f1 = functools.partial(func, 1)
    f2 = functools.partial(func, 2)

    loop.create_task(handle_cronjob("* * * * *", f1, loop, executor))
    loop.create_task(handle_cronjob("*/2 * * * *", f2, loop, executor))

    loop.run_forever()

    print(f"{time.ctime()}: Completed running.")

    executor.shutdown(wait=True)


if __name__ == "__main__":
    main()
