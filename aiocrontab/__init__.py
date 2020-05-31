__version__ = "0.1.2"

from aiocrontab.core import Crontab
from aiocrontab.core import create_logger


__logger = create_logger(namespace="global.aiocrontab.core")
__aiocrontab = Crontab(logger=__logger)
register = __aiocrontab.register
run = __aiocrontab.run

__all__ = [
    "register",
    "run",
    "Crontab",
]
