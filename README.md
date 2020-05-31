# AIOCRONTAB

Sample project to "flex" my asyncio skills.

### Installation
```sh
pip install aiocrontab
```

### Usage

```python
import time

import aiocrontab


@aiocrontab.register("*/5 * * * *")
def print_every_five_mminutes():
    print(f"{time.ctime()}: Hello World!!!!!")

@aiocrontab.register("* * * * *")
def print_every_mminute():
    print(f"{time.ctime()}: Hello World!")


aiocrontab.run()
```

**TODO**

- [x] support for diff timezones
- [x] support for async task
- [x] take logger as dependency
- [ ] Add more meaningful tests
- [x] fix mypy errors
- [ ] document the codebase
- [ ] document usage in readme
- [ ] different namespaces for different crontab instances
- [ ] ability to schedule task from code
- [ ] better logging messages

If you didn't create any new `Crontab` instance (ie: You are using `@aiocrontab.register`), you can get the
logger that the aiocrontab application uses using the following snippet

```python
import logging

logger = logging.getLogger("global.aiocrontab.core")
```

If you created your own Crontab instance(using `Crontab()`), and in-case you didn't provide the logger when creating a `Crontab` instance, you can
use the following snippet to get the logger that it uses.
```python
import logging

logger = logging.getLogger("aiocrontab.core")
```
