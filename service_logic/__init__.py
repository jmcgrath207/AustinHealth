import logging
import os
import pickle
from asyncio import Queue
from pathlib import Path
from time import gmtime
from types import SimpleNamespace

from aiofile import async_open
from httpx import AsyncClient, Timeout, AsyncHTTPTransport
from service_logic.buffer import Buffer


def set_logging() -> logging.Logger:
    """
    Set Logging Levels

    :return:
    """
    logging_format = f'%(asctime)-15s %(levelname)s PID: {os.getpid()}  %(name)s  %(message)s'

    logging.basicConfig(format=logging_format)
    logging.Formatter.converter = gmtime
    logger = logging.getLogger()
    if app.environment == 'PROD':
        logger.setLevel(logging.INFO)

    elif app.environment == 'DEV':
        logger.setLevel(logging.DEBUG)

    elif app.environment == 'LOCAL':
        logger.setLevel(logging.DEBUG)

    return logger


class App(SimpleNamespace):
    """
    Namespace that will be shared across the application

    """
    data_queue: Queue
    environment: str
    parquet_path: Path
    pickle_path: Path
    logger: logging.Logger
    api_session: AsyncClient
    current_offset: int
    buffer: Buffer
    api_pagination_limit: int


app = App()


async def set_current_offset(offset: int) -> None:
    """
    Sets last successful offset. This allow the application to checkpoint
    or restore state based on it's last successful parquet dump.

    :return: int
    """
    async with async_open(str(app.pickle_path), 'wb') as f:
        pickled = pickle.dumps(offset)
        await f.write(pickled)
    app.current_offset = offset


async def get_current_offset() -> int:
    """
    Gets last stored offset. This allow the application to checkpoint
    or restore state based on it's last successful parquet dump.

    :return: int
    """
    # TODO: Not loading checkpoint
    if app.pickle_path.is_file():
        async with async_open(str(app.pickle_path), 'rb') as f:
            pickled = await f.read()
        offset = pickle.loads(pickled)
    else:
        offset = 0
    return offset


async def init() -> None:
    """
    Set properties for the app object

    :return: None
    """
    app.data_queue = Queue()
    app.environment = os.environ.get('ENVIRONMENT', 'LOCAL')
    app.parquet_path = Path().cwd().joinpath('parquet')
    app.pickle_path = Path().cwd().joinpath('offset.pickle')
    app.logger = set_logging()
    app.api_session = AsyncClient(
        headers={
            "Content-Type": "application/json",
        },
        transport=AsyncHTTPTransport(retries=5),
        timeout=Timeout(connect=300, read=300, write=300, pool=5)
    )
    app.current_offset = await get_current_offset()
    app.api_pagination_limit = 500

    app.buffer = Buffer(mb_max_size=1)


async def close_init() -> None:
    """
    Used to close out
    :return:
    """

    pass
