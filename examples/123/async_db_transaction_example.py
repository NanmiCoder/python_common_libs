import asyncio
import logging
import os
import sys
from typing import Any, Dict

project_root = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
)
sys.path.append(project_root)
print(project_root)

from db.async_db_transaction import AsyncDatabase

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# 数据库配置
DB_CONFIG = {
    "host": "localhost",
    "port": 3306,
    "user": os.getenv("MYSQL_USER", "root"),
    "password": os.getenv("RELATION_DB_PWD", ""),
    "db": "test",
    "charset": "utf8mb4",
    "minsize": 1,
    "maxsize": 10,
}


async def init_tables(db: AsyncDatabase):
    """初始化测试表"""
    create_users_table = """
    CREATE TABLE IF NOT EXISTS users (
        id INT AUTO_INCREMENT PRIMARY KEY,
        username VARCHAR(50) NOT NULL,
        email VARCHAR(100) NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """
    await db.execute(create_users_table)


async def example_no_transaction():
    """无事务操作示例"""
    db = AsyncDatabase()
    try:
        # 连接数据库
        await db.connect(**DB_CONFIG, autocommit=True)

        # 插入数据
        user_data = {"username": "test_user", "email": "test@example.com"}
        user_id = await db.insert("users", user_data)
        logger.info(f"插入用户，ID: {user_id}")

        # 查询单条记录
        user = await db.get("SELECT * FROM users WHERE id = %s", user_id)
        logger.info(f"查询用户: {user}")

        # 更新数据
        update_data = {"email": "updated@example.com"}
        affected_rows = await db.update("users", update_data, "id = %s", user_id)
        logger.info(f"更新影响行数: {affected_rows}")

        # 查询多条记录
        users = await db.query("SELECT * FROM users LIMIT 10")
        logger.info(f"查询到 {len(users)} 个用户")

    finally:
        await db.close()


async def example_with_transaction():
    """事务操作示例"""
    db = AsyncDatabase()
    try:
        # 连接数据库，禁用自动提交以支持事务
        await db.connect(**DB_CONFIG, autocommit=False)

        async with await db.get_connection() as conn:
            try:
                # 在事务中执行多个操作
                user1_data = {
                    "username": "transaction_user1",
                    "email": "user1@example.com",
                }
                user1_id = await conn.insert("users", user1_data)
                logger.info(f"事务中插入用户1，ID: {user1_id}")

                user2_data = {
                    "username": "transaction_user2",
                    "email": "user2@example.com",
                }
                user2_id = await conn.insert("users", user2_data)
                logger.info(f"事务中插入用户2，ID: {user2_id}")

                # 模拟可能的错误情况
                if user2_id % 2 == 0:
                    raise ValueError("模拟错误，触发回滚")

                # 如果没有错误，事务将自动提交
                logger.info("事务成功完成")

            except Exception as e:
                logger.error(f"事务出错，将自动回滚: {str(e)}")
                raise  # 重新抛出异常，触发回滚

    finally:
        await db.close()


async def example_batch_operations():
    """批量操作示例"""
    db = AsyncDatabase()
    try:
        await db.connect(**DB_CONFIG, autocommit=False)

        async with await db.get_connection() as conn:
            # 准备批量插入的数据
            users_data = [
                {"username": f"batch_user_{i}", "email": f"batch{i}@example.com"}
                for i in range(5)
            ]

            # 批量插入用户
            inserted_ids = []
            for user_data in users_data:
                user_id = await conn.insert("users", user_data)
                inserted_ids.append(user_id)

            logger.info(f"批量插入的用户ID: {inserted_ids}")

            # 批量查询
            users = await conn.query(
                "SELECT * FROM users WHERE id IN (%s)"
                % ",".join(["%s"] * len(inserted_ids)),
                *inserted_ids,
            )
            logger.info(f"批量查询结果: {users}")

    finally:
        await db.close()


async def main():
    """主函数"""
    db = AsyncDatabase()
    try:
        # 连接数据库并初始化表
        await db.connect(**DB_CONFIG)
        await init_tables(db)

        # 运行示例
        logger.info("=== 运行无事务示例 ===")
        await example_no_transaction()

        logger.info("\n=== 运行事务示例 ===")
        await example_with_transaction()

        logger.info("\n=== 运行批量操作示例 ===")
        await example_batch_operations()

    finally:
        await db.close()


if __name__ == "__main__":
    # 将db加入到sys.path中

    asyncio.run(main())
