# -*- coding: utf-8 -*-
# @Author  : relakkes@gmail.com
# @Time    : 2023/12/10 19:09
# @Desc    : 测试封装的 aiomysql
import os
from typing import Any, Dict, List

import aiomysql
import pytest
import pytest_asyncio

from db import async_db


@pytest_asyncio.fixture()
async def db_conn():
    """
    # 在测试会话开始时初始化数据库连接池
    :return:
    """
    pool = await aiomysql.create_pool(
        host="127.0.1",
        port=3306,
        user="root",
        password=os.getenv("RELATION_DB_PWD", ""),
        db="test",
        autocommit=True
    )
    return async_db.AsyncMysqlDB(pool)


@pytest.mark.asyncio
async def test_create_table(db_conn: async_db.AsyncMysqlDB):
    table_sql: str = """
    CREATE TABLE IF NOT EXISTS `user`(
      `id` int NOT NULL AUTO_INCREMENT COMMENT '自增ID',
      `user_name` varchar(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL COMMENT '用户名',
      `user_phone` varchar(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL COMMENT '用户手机号',
      PRIMARY KEY (`id`) USING BTREE
    ) ENGINE = InnoDB AUTO_INCREMENT = 1;
    """

    rows: int = await db_conn.execute(table_sql)
    assert isinstance(rows, int) and (rows >= 0)


@pytest.mark.asyncio
async def test_insert_data(db_conn: async_db.AsyncMysqlDB):
    rows_data: List[Dict] = [
        {"user_name": "zhangsan", "user_phone": 13100001111},
        {"user_name": "lisi", "user_phone": 13100001112},
        {"user_name": "wangmazi", "user_phone": 13100001113},
    ]
    for row_item in rows_data:
        last_row_id = await db_conn.item_to_table(table_name="user", item=row_item)
        assert last_row_id > 0


@pytest.mark.asyncio
async def test_query_data(db_conn: async_db.AsyncMysqlDB):
    sql: str = "select id, user_phone, user_name from user where id in (1,2) order by id asc limit 2;"
    data_list: List[Dict] = await db_conn.query(sql)
    assert isinstance(data_list, list)
    assert data_list[0]["id"] == 1
    assert data_list[1]["id"] == 2


@pytest.mark.asyncio
async def test_update_data(db_conn: async_db.AsyncMysqlDB):
    update_row_id: int = 2
    update_values: Dict[str, Any] = {
        "user_name": "lisi1",
        "user_phone": "13100001122"
    }
    affect_rows = await db_conn.update_table(table_name="user", updates=update_values, field_where="id", value_where=update_row_id)
    assert affect_rows >= 0

    updated_row_data = await db_conn.get_first("select * from user where id = %s", update_row_id)
    assert updated_row_data.get("user_name") == "lisi1"
    assert updated_row_data.get("user_phone") == "13100001122"
