__version__ = "0.1.1"

from aiocrontab.core import Crontab


_aiocrontab = Crontab()
register = _aiocrontab.register
run = _aiocrontab.run

__all__ = [
    "register",
    "run",
    "Crontab",
]
