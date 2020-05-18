import asyncio
import logging
import os
import signal
import threading
import time

import aiocrontab
import pytest


@pytest.mark.skip
def test_handle_cronjob_is_called_task_is_run(mocker):
    task = mocker.Mock()
    mocker.patch("aiocrontab.register")

    aiocrontab.register("* * * * *")(task)
    aiocrontab.run()

    task.assert_called_once_with()
    aiocrontab.register.assert_called_once_with("* * * * *")


def test_loop_property_is_set_when_first_called(mocker, crontab):
    mocker.patch("aiocrontab.core.asyncio.get_event_loop")
    cron = crontab()

    assert cron._loop is None

    cron.loop()
    assert cron._loop is not None

    aiocrontab.core.asyncio.get_event_loop.assert_called_once_with()


def test_loop_property_calls_initialize_event_loop(mocker, crontab):
    mocker.patch("aiocrontab.core.asyncio.get_event_loop")
    cron = crontab()
    cron.initialize_event_loop = mocker.Mock()

    assert cron.initialize_event_loop.call_count == 0
    cron.loop()
    assert cron.initialize_event_loop.call_count == 1


def test_initialize_event_loop(mocker, crontab):
    cron = crontab(loop=mocker.Mock(set_exception_handler=mocker.Mock()))
    cron.handle_error_signals = mocker.Mock()
    cron.handle_exception = mocker.Mock()

    cron.initialize_event_loop()

    cron.loop.set_exception_handler.assert_called_once_with(
        cron.handle_exception
    )
    cron.handle_error_signals.assert_called_once_with(cron.loop)


def test_executor_property(mocker, crontab):
    mocker.patch("aiocrontab.core.thread.ThreadPoolExecutor")

    cron = crontab()

    assert cron._executor is None

    cron.executor

    assert cron._executor is not None
    aiocrontab.core.thread.ThreadPoolExecutor.assert_called_once_with()


def test_register(mocker, crontab):
    cron = crontab()

    f1 = mocker.Mock()
    f2 = mocker.Mock()

    assert len(cron.registered_tasks) == 0

    cron.register("* * * * *")(f1)

    assert len(cron.registered_tasks) == 1

    cron.register("*/2 * * * *")(f2)

    assert len(cron.registered_tasks) == 2

    task1 = cron.registered_tasks[0]
    task2 = cron.registered_tasks[1]

    task1["pattern"] == "* * * * *"
    task2["pattern"] == "*/2 * * * *"

    task1["func"] is f1
    task2["func"] is f2


def test_exceptions_are_logged(mock_crontab_with_tasks, create_caplog):
    mock_crontab, f1 = mock_crontab_with_tasks(mock_handle_cronjob=True)
    caplog = create_caplog(logging.ERROR)
    mock_crontab.run()
    assert (
        mock_crontab.handle_exception
        == mock_crontab.loop.get_exception_handler()
    )
    assert 2 == len(caplog.records)


@pytest.mark.parametrize("tested_signal", ["SIGINT", "SIGUSR1"])
def test_shutdown_is_called_on_receiving_error_signals(
    tested_signal, mocker, mock_crontab_with_tasks, event_loop
):
    tested_signal = getattr(signal, tested_signal)
    mock_crontab, f1 = mock_crontab_with_tasks(mock_handle_cronjob=False)
    mock_shutdown = mocker.Mock()
    event_loop = asyncio.get_event_loop_policy().new_event_loop()
    asyncio.set_event_loop(event_loop)
    event_loop._close = event_loop.close
    event_loop.close = mocker.Mock()

    def _shutdown():
        mock_shutdown()
        event_loop.stop()

    event_loop.add_signal_handler(tested_signal, _shutdown)

    def _send_signal():
        time.sleep(0.1)
        os.kill(os.getpid(), tested_signal)

    thread = threading.Thread(target=_send_signal, daemon=True)
    thread.start()

    mock_crontab.run()

    assert tested_signal in event_loop._signal_handlers

    # shutdown is not called for unregistered signals
    if tested_signal is not signal.SIGUSR1:
        mock_shutdown.assert_not_called()
    else:
        mock_shutdown.assert_called_once_with()

    # asserting the loop is stopped but not closed
    assert not event_loop.is_running()
    assert not event_loop.is_closed()
    event_loop.close.assert_called_once_with()

    event_loop._close()
