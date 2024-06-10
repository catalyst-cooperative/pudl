"""A place for helper functions that *don't* import PUDL and thus can be imported anywhere."""

import logging
import time
from collections.abc import Callable

logger = logging.getLogger(f"catalystcoop.{__name__}")


def retry(
    func: Callable,
    retry_on: tuple[type[BaseException], ...],
    max_retries=5,
    base_delay_sec=1,
    **kwargs,
):
    """Retry a function with a short sleep between each try.

    Sleeps twice as long before each retry as the last one, e.g. 1/2/4/8/16
    seconds.

    Args:
    func: the function to retry
    retry_on: the errors to catch.
    base_delay_sec: how much time to sleep for the first retry.
    kwargs: keyword arguments to pass to the wrapped function. Pass non-kwargs as kwargs too.
    """
    for try_count in range(max_retries):
        delay = 2**try_count * base_delay_sec
        try:
            return func(**kwargs)
        except retry_on as e:
            logger.info(
                f"{e}: retry in {delay}s. {try_count}/{max_retries} retries used."
            )
            time.sleep(delay)
    return func(**kwargs)
