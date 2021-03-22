import asyncio

from service_logic import init, close_init
from service_logic.normalization import normalization
from service_logic.restaurant_inspections import fetch


async def main() -> None:
    """
    Gather all coroutines, submit them to the event
    loop then waits for all to complete. If one
    raises a exception, all coroutines are cancelled.

    :return: None
    """
    try:
        await init()
        coroutine_list = [normalization(), fetch()]
        await asyncio.gather(*coroutine_list)
    except Exception as e:
        await close_init()
        raise e
    finally:
        await close_init()


if __name__ == '__main__':
    asyncio.run(main())


