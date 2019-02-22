import os
import asyncio
import aiomysql
from functools import wraps
from typing import List, Union, Dict, Any, Iterable, Callable
from aiomysql import InterfaceError


CONNECTION_INFO = {
    'use_unicode': True,
    'charset': 'utf8mb4',
    'user': os.environ.get('MYSQL_USER'),
    'password': os.environ.get('MYSQL_PASSWORD'),
    'db': os.environ.get('MYSQL_DATABASE'),
    'host': os.environ.get('MYSQL_HOST'),
    'port': os.environ.get('MYSQL_PORT'),
    'loop': None,
    'autocommit': True
}


def reconnection_pool():
    def inner(fn):
        @wraps(fn)
        async def wrapper(*args, **kwargs):
            try:
                return await fn(*args, **kwargs)
            except Exception as e:
                if isinstance(e, InterfaceError) and e.args[0] == 'pool is closed':
                    args[0].pool = None
                    return await fn(*args, **kwargs)
                else:
                    raise e
        return wrapper


class MySQLConnection:
    pool: aiomysql.Pool = None
    lock: asyncio.Lock = None

    @staticmethod
    async def initialize(connection_info=None):
        MySQLConnection.lock = asyncio.Lock()
        await MySQLConnection.get_pool(connection_info)

    @staticmethod
    async def destroy():
        if MySQLConnection.pool is not None:
            MySQLConnection.pool.close()
            await MySQLConnection.pool.wait_closed()

        MySQLConnection.pool = None

    @classmethod
    async def get_pool(cls, connection_info=None) -> aiomysql.Pool:
        if not cls.pool:
            with await cls.lock:
                if cls.pool is None:
                    cls.pool = await aiomysql.create_pool(**connection_info) if connection_info else await aiomysql.create_pool(**CONNECTION_INFO)

        return cls.pool

    @classmethod
    async def execute(cls, query: str, *args) -> int:
        pool: aiomysql.Pool = await cls.get_pool()
        conn: aiomysql.Connection
        cur: aiomysql.DictCursor
        result: int

        async with pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                result = await cur.execute(query, args)

        return result

    @classmethod
    async def executemany(cls, query: str, *args) -> int:
        pool: aiomysql.Pool = await cls.get_pool()
        conn: aiomysql.Connection
        cur: aiomysql.DictCursor
        result: int

        async with pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                result = await cur.executemany(query, args)

        return result

    @classmethod
    async def fetch(cls, query: str, *args) -> List[Dict[str, Any]]:
        pool: aiomysql.Pool = await cls.get_pool()
        conn: aiomysql.Connection
        cur: aiomysql.DictCursor
        result: List[Dict[str, Any]]

        async with pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute(query, args)
                result = await cur.fetchall()

        return result

    @classmethod
    async def fetchone(cls, query: str, *args) -> Dict[str, Any]:
        pool: aiomysql.Pool = await cls.get_pool()
        conn: aiomysql.Connection
        cur: aiomysql.DictCursor
        result: Dict[str, Any]

        async with pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute(query, args)
                result = await cur.fetchone()

        return result

