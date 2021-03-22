import asyncio

from httpx import Response

from service_logic import app


async def fetch() -> None:
    """
    Fetch restaurant inspection reports

    :return: None
    """
    limit: int = app.api_pagination_limit
    offset: int = app.current_offset
    # Limit concurrency to two coroutines at a time.
    api_request_lock = asyncio.Semaphore(value=2)
    while True:
        async with api_request_lock:

            if app.buffer.is_full:
                await asyncio.sleep(1)
                continue

            response: Response = await app.api_session.get('https://data.austintexas.gov/resource/ecmv-9xxi.json',
                                                           params={'$limit': limit, '$offset': offset})
            if response.status_code != 200:
                raise ConnectionError(f'Failed to reach out to {response.url}\nStatus code: {response.status_code}')

            await asyncio.sleep(3)

        complete: bool = await app.buffer.to_data_queue(offset=offset, response=response)
        if complete:
            break
        offset = offset + limit
