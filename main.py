from asyncio import new_event_loop, set_event_loop_policy, WindowsSelectorEventLoopPolicy, gather, create_task, Task, Queue, sleep as asleep
from os.path import isfile
from threading import Thread
from typing import Optional

from aiofiles import open as aopen
from aiohttp import ClientSession
from aiosqlite import connect, Connection
from bs4 import BeautifulSoup
from orjson import dumps, loads, OPT_INDENT_2

HEADERS = {
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36 OPR/93.0.0.0 (Edition GX-CN)"
}
BS_FEATURES = "html.parser"
KEY_TUPLE = (
    "成交",
    "開盤",
    "最高",
    "最低",
    "均價",
    "成交金額(億)",
    "昨收",
    "漲跌幅",
    "漲跌",
    "總量",
    "昨量",
    "振幅"
)
k = 0
total = 0

async def main():
    global total
    if not isfile("data.db"):
        async with connect("data.db") as db:
            async with db.cursor() as cursor:
                await cursor.execute("""
                CREATE TABLE "data" (
                    "id" TEXT NOT NULL UNIQUE,
                    "成交" TEXT,
                    "開盤" TEXT,
                    "最高" TEXT,
                    "最低" TEXT,
                    "均價" TEXT,
                    "成交金額(億)" TEXT,
                    "昨收" TEXT,
                    "漲跌幅" TEXT,
                    "漲跌" TEXT,
                    "總量" TEXT,
                    "昨量" TEXT,
                    "振幅" TEXT,
                    PRIMARY KEY("id")
                );
                """)
            await db.commit()

    print("取得所有號碼...")
    session = ClientSession(headers=HEADERS)
    if not isfile("total_id.json"):
        total_id = await get_all_list(session=session)
        async with aopen("total_id.json", mode="wb") as _file:
            await _file.write(dumps(total_id, option=OPT_INDENT_2))
    else:
        async with aopen("total_id.json") as _file:
            total_id = loads(await _file.read())
    total = len(total_id)
    # async with ClientSession(headers=HEADERS) as client:
    #     tasks = list(map(lambda _id: create_task(client.get(f"https://tw.stock.yahoo.com/quote/{_id}")), total_id))
    #     print("Start")
    #     await gather(*tasks)
    
    print("取得所有資料...")
    queue = Queue()
    Thread(target=insert_db_thread, args=(queue,)).start()
    lim = True
    if lim:
        in_queue = Queue()
        for id_ in total_id: in_queue.put_nowait(id_)
        tasks = [
            create_task(queue_data(
                in_queue,
                queue,
                session
            ))
            for _ in range(1000)
        ]
        await gather(*tasks)
    else:
        tasks = list(map(lambda id_: create_task(get_data(id_=id_, queue=queue, session=session)), total_id))
        await gather(*tasks)

    await session.close()

async def queue_data(in_queue: Queue, out_queue: Queue, session: Optional[ClientSession]=None):
    while not in_queue.empty():
        id_ = await in_queue.get()
        await get_data(id_, out_queue, session)

async def get_data(id_: str, queue: Queue, session: Optional[ClientSession]=None):
    res = await _requests(url=f"https://tw.stock.yahoo.com/quote/{id_}", session=session)
    soup = BeautifulSoup(res, features=BS_FEATURES)
    r_data = {}
    for data in soup.select("li.price-detail-item"):
        tag_list = data.select("span")
        key = tag_list[0].text
        value = tag_list[1].text
        if key not in KEY_TUPLE: continue
        r_data[key] = value
    await queue.put((id_, r_data))

def insert_db_thread(queue: Queue):
    loop = new_event_loop()
    loop.run_until_complete(insert_db(queue))
    loop.close()

async def insert_db(queue: Queue):
    global k
    async with connect("data.db") as db:
        async with db.cursor() as cursor:
            while True:
                if queue.empty():
                    await asleep(1)
                    continue
                id_, data = await queue.get()
                await cursor.execute(
                    f"SELECT * FROM data WHERE id=:id",
                    {"id": id_}
                )
                if await cursor.fetchone() == None:
                    await cursor.execute(
                        f"INSERT INTO data (id) VALUES (:id)",
                        {"id": id_}
                    )
                for key, value in data.items():
                    await cursor.execute(
                        f"UPDATE data SET \"{key}\"=:value WHERE id=:id",
                        {"value": value, "id": id_}
                    )
                await db.commit()
                k += 1
                print(f"{k} / {total} - {id_}", end="\r")

async def get_all_list(session: Optional[ClientSession]=None):
    type_names = map(
        lambda res: map(
            lambda tag: tag.text,
            BeautifulSoup(res, features=BS_FEATURES).select("td:not([rowspan]) > a[href*='/h/kimosel.php']"),
        ),
        await gather(
            create_task(_requests(url=f"https://tw.stock.yahoo.com/h/kimosel.php?tse=1&cat=%", session=session)),
            create_task(_requests(url=f"https://tw.stock.yahoo.com/h/kimosel.php?tse=2&cat=%", session=session)),
        ),
    )

    total_tasks: list[Task] = []
    for i, name_list in enumerate(type_names, 1):
        total_tasks += list(map(
            lambda name: create_task(get_ids(url=f"https://tw.stock.yahoo.com/h/kimosel.php?tse={i}&cat={name}", session=session)),
            name_list
        ))
    
    result = []
    for res in await gather(*total_tasks):
        result += res
    
    return result

async def get_ids(url: str, session: Optional[ClientSession]=None) -> list[str]:
    soup = BeautifulSoup(await _requests(url=url, session=session), features=BS_FEATURES)

    # return None
    return list(map(lambda tag: tag.text.split(" ", 1)[0].strip(), soup.select("a[href*='javascript:setid']")))

async def _requests(url: str, session: Optional[ClientSession]=None):
    __need_close = False
    if session == None:
        __need_close = True
        session = ClientSession(headers=HEADERS)

    try:
        res = await session.get(url)
        res = await res.content.read()
    except:
        res = await _requests(url=url, session=session)
    
    if __need_close:
        await session.close()

    return res

if __name__ == "__main__":
    set_event_loop_policy(WindowsSelectorEventLoopPolicy())
    from time import time_ns
    
    timer = time_ns()
    loop = new_event_loop()
    loop.run_until_complete(main())
    loop.close()
    print(f"Total Time: {format((time_ns() - timer) * (10 ** -9), '.2f')}s")