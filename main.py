import asyncio

from service_logic import init, close_init
from service_logic.dump_parquet import dump_parquet
from service_logic.restaurant_inspections import fetch


async def main():
    try:
        await init()
        asyncio.create_task(dump_parquet())
        await fetch()
    except Exception as e:
        await close_init()
        raise e
    finally:
        await close_init()


if __name__ == '__main__':
    asyncio.run(main())


