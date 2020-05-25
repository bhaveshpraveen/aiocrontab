# AIOCRONTAB

Sample project to "flex" my asyncio skills.


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

- [ ] support for diff timezones
- [ ] support for async task
- [x] take logger as dependency
- [ ] Add more meaningful tests
- [x] fix mypy errors
