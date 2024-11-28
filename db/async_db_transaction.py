import logging
from typing import Any, Dict, List, Optional, Union, Tuple

import aiomysql

logger = logging.getLogger(__name__)


class DatabaseConnection:
    """数据库连接上下文管理器。

    用于管理单个数据库连接的生命周期和事务。

    Attributes:
        pool (aiomysql.Pool): 数据库连接池
        conn (aiomysql.Connection): 数据库连接
        autocommit (bool): 是否自动提交
        _in_transaction (bool): 是否在事务中
    """

    def __init__(
        self, pool: aiomysql.Pool, conn: aiomysql.Connection, autocommit: bool
    ) -> None:
        """初始化数据库连接管理器。

        Args:
            pool: 数据库连接池
            conn: 数据库连接
            autocommit: 是否自动提交
        """
        self.pool = pool
        self.conn = conn
        self.autocommit = autocommit
        self._in_transaction = False

    async def __aenter__(self) -> "DatabaseConnection":
        """异步上下文管理器入口。

        Returns:
            DatabaseConnection: 返回自身实例
        """
        if not self.autocommit:
            await self.begin()
        return self

    async def __aexit__(
        self,
        exc_type: Optional[type],
        exc_val: Optional[Exception],
        exc_tb: Optional[Any],
    ) -> None:
        """异步上下文管理器退出。

        Args:
            exc_type: 异常类型
            exc_val: 异常值
            exc_tb: 异常回溯
        """
        try:
            if self._in_transaction:
                if exc_type is not None:
                    await self.rollback()
                else:
                    await self.commit()
        finally:
            await self.pool.release(self.conn)

    async def begin(self) -> None:
        """开始事务。"""
        await self.conn.begin()
        self._in_transaction = True

    async def commit(self) -> None:
        """提交事务。"""
        await self.conn.commit()
        self._in_transaction = False

    async def rollback(self) -> None:
        """回滚事务。"""
        await self.conn.rollback()
        self._in_transaction = False

    async def query(self, sql: str, *args) -> List[Dict[str, Any]]:
        """执行查询，返回多条记录。

        Args:
            sql: SQL查询语句
            *args: SQL参数

        Returns:
            List[Dict[str, Any]]: 查询结果列表
        """
        async with self.conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(sql, args or None)
            data = await cur.fetchall()
            return list(data) if data else []

    async def get(self, sql: str, *args) -> Optional[Dict[str, Any]]:
        """执行查询，返回单条记录。

        Args:
            sql: SQL查询语句
            *args: SQL参数

        Returns:
            Optional[Dict[str, Any]]: 查询结果，未找到时返回None
        """
        async with self.conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(sql, args or None)
            return await cur.fetchone()

    async def execute(self, sql: str, *args) -> int:
        """执行更新操作。

        Args:
            sql: SQL更新语句
            *args: SQL参数

        Returns:
            int: 影响行数
        """
        async with self.conn.cursor() as cur:
            rows = await cur.execute(sql, args or None)
            if self.autocommit:
                await self.conn.commit()
            return rows

    async def insert(self, table: str, data: Dict[str, Any]) -> int:
        """插入数据。

        Args:
            table: 表名
            data: 数据字典

        Returns:
            int: 新记录ID
        """
        fields = list(data.keys())
        values = list(data.values())
        placeholders = ",".join(["%s"] * len(data))
        sql = f"INSERT INTO {table} ({','.join(fields)}) VALUES ({placeholders})"

        async with self.conn.cursor() as cur:
            await cur.execute(sql, values)
            if self.autocommit:
                await self.conn.commit()
            return cur.lastrowid

    async def update(self, table: str, data: Dict[str, Any], where: str, *args) -> int:
        """更新数据。

        Args:
            table: 表名
            data: 更新的数据字典
            where: WHERE条件
            *args: WHERE条件的参数

        Returns:
            int: 影响行数
        """
        set_items = [f"{k}=%s" for k in data.keys()]
        values = list(data.values()) + list(args)
        sql = f"UPDATE {table} SET {','.join(set_items)} WHERE {where}"
        return await self.execute(sql, *values)


class AsyncDatabase:
    """异步MySQL数据库操作类。

    提供异步数据库操作的高级接口，包括连接池管理和事务支持。

    Attributes:
        __pool (Optional[aiomysql.Pool]): 数据库连接池
        __autocommit (bool): 是否自动提交
    """

    def __init__(self) -> None:
        """初始化数据库操作类。"""
        self.__pool = None
        self.__autocommit = True

    async def connect(
        self,
        host: str = "localhost",
        port: int = 3306,
        user: Optional[str] = None,
        password: Optional[str] = None,
        db: Optional[str] = None,
        charset: str = "utf8mb4",
        minsize: int = 1,
        maxsize: int = 10,
        autocommit: bool = True,
        **kwargs: Any,
    ) -> None:
        """建立数据库连接池。

        Args:
            host: 数据库主机地址
            port: 数据库端口
            user: 用户名
            password: 密码
            db: 数据库名
            charset: 字符集
            minsize: 连接池最小连接数
            maxsize: 连接池最大连接数
            autocommit: 是否自动提交
            **kwargs: 其他aiomysql支持的参数

        Raises:
            Exception: 创建连接池失败时抛出
        """
        try:
            pool_config = {
                "host": host,
                "port": port,
                "user": user,
                "password": password,
                "db": db,
                "charset": charset,
                "minsize": minsize,
                "maxsize": maxsize,
                "autocommit": True,  # 连接池级别总是自动提交，事务由 DatabaseConnection 控制
                **kwargs,
            }
            self.__pool = await aiomysql.create_pool(**pool_config)
            self.__autocommit = autocommit
            logger.info("MySQL connection pool created")
        except Exception as e:
            logger.error(f"Failed to create MySQL connection pool: {str(e)}")
            raise

    async def get_connection(self) -> DatabaseConnection:
        """获取数据库连接上下文管理器。

        Returns:
            DatabaseConnection: 数据库连接管理器实例

        Raises:
            RuntimeError: 连接池未初始化时抛出
        """
        if self.__pool is None:
            raise RuntimeError("Database connection pool is not initialized")
        conn = await self.__pool.acquire()
        await conn.begin()  # 开始一个事务
        await conn.commit()  # 立即提交以清除任何未完成的事务
        return DatabaseConnection(self.__pool, conn, self.__autocommit)

    async def query(self, sql: str, *args) -> List[Dict[str, Any]]:
        """执行查询，返回多条记录。

        Args:
            sql: SQL查询语句
            *args: SQL参数

        Returns:
            List[Dict[str, Any]]: 查询结果列表
        """
        async with await self.get_connection() as db:
            return await db.query(sql, *args)

    async def get(self, sql: str, *args) -> Optional[Dict[str, Any]]:
        """执行查询，返回单条记录。

        Args:
            sql: SQL查询语句
            *args: SQL参数

        Returns:
            Optional[Dict[str, Any]]: 查询结果，未找到时返回None
        """
        async with await self.get_connection() as db:
            return await db.get(sql, *args)

    async def execute(self, sql: str, *args) -> int:
        """执行更新操作。

        Args:
            sql: SQL更新语句
            *args: SQL参数

        Returns:
            int: 影响行数
        """
        async with await self.get_connection() as db:
            return await db.execute(sql, *args)

    async def insert(self, table: str, data: Dict[str, Any]) -> int:
        """插入数据。

        Args:
            table: 表名
            data: 数据字典

        Returns:
            int: 新记录ID
        """
        async with await self.get_connection() as db:
            return await db.insert(table, data)

    async def update(self, table: str, data: Dict[str, Any], where: str, *args) -> int:
        """更新数据。

        Args:
            table: 表名
            data: 更新的数据字典
            where: WHERE条件
            *args: WHERE条件的参数

        Returns:
            int: 影响行数
        """
        async with await self.get_connection() as db:
            return await db.update(table, data, where, *args)

    async def close(self) -> None:
        """关闭连接池。"""
        if self.__pool is not None:
            self.__pool.close()
            await self.__pool.wait_closed()
            self.__pool = None
            logger.info("MySQL connection pool closed")
