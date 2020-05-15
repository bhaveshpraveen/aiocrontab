from aiocrontab import __version__
from aiocrontab.task import crontab


def test_version():
    assert __version__ == "0.1.0"


def test_task_is_called_when_decorated_with_crontab(mocker):

    task = mocker.Mock(name="task", return_value=True)

    decorated_task = crontab("* * * * *")(task)
    ret = decorated_task("test1", "test2")

    task.assert_called_with("test1", "test2")
    assert task.call_count == 1

    assert ret is True
