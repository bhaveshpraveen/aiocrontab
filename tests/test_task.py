from datetime import datetime

import aiocrontab.core
import pytest
from aiocrontab.core import Task


def test_time_functions_properly(mocker):
    task = Task(
        pattern="* * * * *",
        func=mocker.Mock(),
        loop=mocker.Mock(),
        executor=mocker.Mock(),
    )
    assert task._time is None

    # time is now set
    now = task.time
    assert task._time == now
    assert isinstance(now, datetime)

    # the time is the same on subsequent calls when it was first set
    second_call = task.time
    assert now == second_call
    assert task._time == now


def test_next_timestamp(mocker, mock_croniter):
    task = Task(
        pattern="* * * * *",
        func=mocker.Mock(),
        loop=mocker.Mock(),
        executor=mocker.Mock(),
    )

    mocker.patch("aiocrontab.core.croniter", mock_croniter)
    assert task._next_timestamp is None

    # next_timestamp is now set
    timestamp_now = task.next_timestamp
    assert timestamp_now == 500.00
    assert isinstance(timestamp_now, float)

    # next_timestamp is the same on subsequent calls
    timestamp_latest = task.next_timestamp
    assert isinstance(timestamp_latest, float)
    assert timestamp_now == timestamp_latest

    t1 = task.time
    aiocrontab.core.croniter.assert_called_once_with("* * * * *", t1)
    aiocrontab.core.croniter.return_value.get_next.assert_called_once_with(
        ret_type=float
    )


def test_next_loop_timestamp(mocker):
    task = Task(
        pattern="* * * * *",
        func=mocker.Mock(),
        loop=mocker.Mock(time=mocker.Mock(return_value=1.0)),
        executor=mocker.Mock(),
    )

    task._time = mocker.Mock(timestamp=mocker.Mock(return_value=0.0))
    task._next_timestamp = 0.0

    # next_loop_timstamp is not set
    assert task._next_loop_timestamp is None

    # next_loop_timestamp is set
    next_loop_timestamp_now = task.next_loop_timestamp
    assert isinstance(next_loop_timestamp_now, float)
    assert next_loop_timestamp_now == 1.0
    assert task.loop.time.call_count == 1

    # next_loop_timestamp is the same on subsequent calls
    next_loop_timestamp_latest = task.next_loop_timestamp
    assert next_loop_timestamp_now == 1.0
    assert next_loop_timestamp_latest == 1.0
    assert task.loop.time.call_count == 1


@pytest.mark.asyncio
async def test_sleep_until_task_completion(
    event_loop, mocker, create_mock_coro
):
    task = Task(
        pattern="* * * * *",
        func=mocker.Mock(),
        loop=event_loop,
        executor=mocker.Mock(),
    )
    mock, coro = create_mock_coro("aiocrontab.core.asyncio.sleep")
    await task.sleep_until_task_completion(sleep_time=10.0)

    mock.assert_called_once_with(10.0)


@pytest.mark.asyncio
async def test_complete_task_lifecycle(event_loop, mocker, create_mock_coro):
    task = Task(
        pattern="* * * * *",
        func=mocker.Mock(),
        loop=event_loop,
        executor=mocker.Mock(),
    )
    task._time = mocker.Mock(timestamp=mocker.Mock(return_value=0.0))
    task._next_timestamp = 5.0

    mock_schedule, task.schedule = create_mock_coro()
    (
        mock_sleep_until_task_completion,
        task.sleep_until_task_completion,
    ) = create_mock_coro()

    await task.complete_task_lifecycle()

    mock_schedule.assert_called_once_with()
    sleep_time = task._next_timestamp - task._time.timestamp()
    mock_sleep_until_task_completion.assert_called_once_with(
        sleep_time=sleep_time
    )


@pytest.mark.asyncio
async def test_schedule(mocker):
    task = Task(
        pattern="* * * * *",
        func=mocker.Mock(),
        loop=mocker.Mock(call_at=mocker.Mock()),
        executor=mocker.Mock(),
    )
    mock_run = mocker.Mock()
    task.run = mock_run
    task._next_loop_timestamp = 0.0

    await task.schedule()

    task.loop.call_at.assert_called_once_with(0.0, mock_run)


def test_run(mocker):
    task = Task(
        pattern="* * * * *",
        func=mocker.Mock(),
        loop=mocker.Mock(run_in_executor=mocker.Mock()),
        executor=mocker.Mock(),
    )
    task.run()

    task.loop.run_in_executor.assert_called_once_with(task.executor, task.func)


@pytest.mark.asyncio
async def test_handle_cronjob(mocker, create_mock_coro):
    _, mock_coro = create_mock_coro()
    mock_complete_task_lifecycle = mocker.Mock()
    mock_complete_task_lifecycle.side_effect = [
        mock_coro(),
        Exception("Something went wrong."),
    ]
    mock_task_class = mocker.Mock(
        return_value=mocker.Mock(
            complete_task_lifecycle=mock_complete_task_lifecycle
        )
    )
    mocker.patch("aiocrontab.core.Task", mock_task_class)

    mock_func = mocker.Mock()
    # aiocrontab.core.handle_cronjob("* * * * *", mock_func, loop=mocker.Mock(), executor=mocker.Mock())

    with pytest.raises(Exception, match="Something went wrong."):
        await aiocrontab.core.handle_cronjob(
            "* * * * *", mock_func, loop=mocker.Mock(), executor=mocker.Mock()
        )

    assert mock_complete_task_lifecycle.call_count == 2
    assert mock_task_class.call_count == 2
