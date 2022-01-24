import asyncio
import pytest


@pytest.fixture(scope="module")
def loop():
    loop = asyncio.get_event_loop()
    yield loop
    loop.close()
