# -*- coding: utf-8 -*-
import os
from typing import Any, Dict, List

import pytest
import pytest_asyncio

from db.async_db_transaction import AsyncDatabase


@pytest_asyncio.fixture()
async def db():
    """初始化数据库连接"""
    database = AsyncDatabase()
    await database.connect(
        host="127.0.0.1",
        port=3306,
        user="root",
        password=os.getenv("MYSQL_PASSWORD", ""),
        db="test",
        autocommit=False,
    )

    # 设置测试表
    await setup_test_table(database)

    yield database

    # 清理测试数据
    await cleanup_test_table(database)
    await database.close()


async def setup_test_table(db: AsyncDatabase):
    """创建测试表"""
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS `users` (
        `id` int NOT NULL AUTO_INCREMENT,
        `name` varchar(64) NOT NULL,
        `age` int NOT NULL,
        `status` varchar(20) DEFAULT 'inactive',
        PRIMARY KEY (`id`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """
    await db.execute("DROP TABLE IF EXISTS users;")
    await db.execute(create_table_sql)


async def cleanup_test_table(db: AsyncDatabase):
    """清理测试表"""
    await db.execute("DROP TABLE IF EXISTS users;")


@pytest.mark.asyncio
async def test_single_insert(db: AsyncDatabase):
    """测试单条插入"""
    user_id = await db.insert("users", {"name": "张三", "age": 25, "status": "active"})
    assert user_id > 0

    user = await db.get("SELECT * FROM users WHERE id = %s", user_id)
    assert user["name"] == "张三"
    assert user["age"] == 25


@pytest.mark.asyncio
async def test_single_update(db: AsyncDatabase):
    """测试单条更新"""
    user_id = await db.insert(
        "users", {"name": "李四", "age": 30, "status": "inactive"}
    )

    rows = await db.update("users", {"age": 31, "status": "active"}, "id = %s", user_id)
    assert rows == 1

    user = await db.get("SELECT * FROM users WHERE id = %s", user_id)
    assert user["age"] == 31
    assert user["status"] == "active"


@pytest.mark.asyncio
async def test_query_multiple_rows(db: AsyncDatabase):
    """测试查询多条记录"""
    # 插入测试数据
    await db.insert("users", {"name": "用户1", "age": 20})
    await db.insert("users", {"name": "用户2", "age": 25})
    await db.insert("users", {"name": "用户3", "age": 30})

    # 测试查询
    users = await db.query("SELECT * FROM users WHERE age >= %s ORDER BY age", 25)
    assert len(users) == 2
    assert users[0]["age"] == 25
    assert users[1]["age"] == 30


@pytest.mark.asyncio
async def test_transaction_commit(db: AsyncDatabase):
    """测试事务提交"""
    async with await db.get_connection() as conn:
        # 在事务中执行多个操作
        user_id = await conn.insert(
            "users", {"name": "王五", "age": 35, "status": "active"}
        )
        await conn.update("users", {"age": 36}, "id = %s", user_id)

    # 验证事务提交后的结果
    user = await db.get("SELECT * FROM users WHERE id = %s", user_id)
    assert user["name"] == "王五"
    assert user["age"] == 36


@pytest.mark.asyncio
async def test_transaction_rollback(db: AsyncDatabase):
    """测试事务回滚"""
    # 先插入一条数据
    user_id = await db.insert("users", {"name": "赵六", "age": 40, "status": "active"})

    try:
        async with await db.get_connection() as conn:
            await conn.update("users", {"age": 41}, "id = %s", user_id)
            # 故意抛出异常触发回滚
            raise ValueError("测试回滚")
    except ValueError:
        pass

    # 验证数据回滚
    user = await db.get("SELECT * FROM users WHERE id = %s", user_id)
    assert user["age"] == 40  # 应该保持原值


@pytest.mark.asyncio
async def test_transaction_isolation(db: AsyncDatabase):
    """测试事务隔离性"""
    # 准备测试数据
    await db.insert("users", {"name": "测试用户", "age": 50, "status": "active"})

    async with await db.get_connection() as conn:
        # 在事务中修改数据
        await conn.execute("UPDATE users SET age = age + 1 WHERE name = %s", "测试用户")

        # 在事务外查询，应该看不到修改
        user = await db.get("SELECT * FROM users WHERE name = %s", "测试用户")
        assert user["age"] == 50  # 事务外应该看到原值

    # 事务提交后，应该能看到修改
    user = await db.get("SELECT * FROM users WHERE name = %s", "测试用户")
    assert user["age"] == 51


@pytest.mark.asyncio
async def test_connection_reuse(db: AsyncDatabase):
    """测试连接复用"""
    async with await db.get_connection() as conn:
        # 执行多个操作，应该使用同一个连接
        id1 = await conn.insert("users", {"name": "用户A", "age": 60})
        id2 = await conn.insert("users", {"name": "用户B", "age": 61})

        # 在同一个连接中查询
        users = await conn.query(
            "SELECT * FROM users WHERE id in (%s, %s) ORDER BY id", id1, id2
        )
        assert len(users) == 2
        assert users[0]["name"] == "用户A"
        assert users[1]["name"] == "用户B"


@pytest.mark.asyncio
async def test_autocommit_behavior(db: AsyncDatabase):
    """测试自动提交行为"""
    # 创建一个非自动提交的连接
    db_no_autocommit = AsyncDatabase()
    await db_no_autocommit.connect(
        host="127.0.0.1",
        port=3306,
        user="root",
        password=os.getenv("MYSQL_PASSWORD", ""),
        db="test",
        autocommit=False,
    )

    try:
        async with await db_no_autocommit.get_connection() as conn:
            user_id = await conn.insert("users", {"name": "测试自动提交", "age": 70})
            # 不需要显式提交，会在退出上下文时自动提交

        # 验证数据已提交
        user = await db.get("SELECT * FROM users WHERE id = %s", user_id)
        assert user["name"] == "测试自动提交"
    finally:
        await db_no_autocommit.close()


@pytest.mark.asyncio
async def test_error_handling(db: AsyncDatabase):
    """测试错误处理"""
    with pytest.raises(RuntimeError):
        # 测试未初始化连接池的错误
        db_error = AsyncDatabase()
        await db_error.get_connection()

    with pytest.raises(Exception):
        # 测试SQL语法错误
        await db.execute("SELECT * FROM non_existent_table")
