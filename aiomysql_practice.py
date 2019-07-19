# _*_coding:utf-8_*_
#  创建时间: 2019/7/9  9:45


import asyncio
import logging
from mysql_db_config import user, password

import aiomysql

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-8s: %(message)s")

datalist = []

with open('douban_top250_1.txt', encoding='utf-8') as f:
    datalist.extend([i.replace("\n", "").split(",") for i in f.readlines()[1:]])

logging.info(f"datalist   {datalist}")


async def connect_mysql(loop):
    # 链接数据库
    pool = await aiomysql.create_pool(host='localhost', port=3306,
                                      user=user, password=password,
                                      db='valora_190708', charset='utf8', loop=loop)

    # Rank, Title, Director, Actors, Year, Country, Categories, Score, Number, Comments

    # 创建数据表sql语句
    sql_create_table = "create table if not exists aio_mysql_table(" \
                       "Rank varchar(50) comment '电影排名'," \
                       "Title varchar(50) comment '电影名称'," \
                       "Director varchar(100) comment '导演'," \
                       "Actors varchar(100) comment '演员'," \
                       "Years varchar(50) comment '上映年份'," \
                       "Country varchar(100) comment '上映国家'," \
                       "Categories varchar(50) comment '电影类型'," \
                       "Score varchar(50) comment '评分'," \
                       "Numbers varchar(50) comment '评论人数'," \
                       "Comments varchar(100) comment '影评'" \
                       ")DEFAULT charset='utf8';"

    # 添加数据sql语句
    sql_insert_into = "insert into aio_mysql_table(Rank,Title,Director,Actors,Years,Country,Categories,Score,Numbers,Comments) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"

    # 更新数据sql语句
    sql_update_database = "update aio_mysql_table set Comments='非常棒' where Rank='1';"

    # 查询数据sql语句
    sql_select = "select * from aio_mysql_table where Rank <= 100;"

    # 删除数据sql语句
    sql_delete = "delete from aio_mysql_table where Rank > 200;"

    async with pool.acquire() as conn:
        async with conn.cursor() as cursor:
            # await cursor.execute(sql_create_table)  #创建数据表

            # await cursor.executemany(sql_insert_into,datalist)    #插入数据

            # await cursor.execute(sql_update_database)   #更新数据

            # await cursor.execute(sql_select)    #查询数据
            # logging.info(f"await cursor.execute(sql_select)   {await cursor.execute(sql_select)}")

            # 展示查询的所有数据
            # select_results=await cursor.fetchall()
            # for select_result in select_results:
            #     logging.info(f"select_result  {result}")

            # await cursor.execute(sql_delete)    #删除数据

            # 插入数据   备选方案
            ## for i in datalist:
            ##     await cursor.execute(sql_insert_into, i)
            pass

        await conn.commit()
    pool.close()
    await pool.wait_closed()


loop = asyncio.get_event_loop()
loop.run_until_complete(connect_mysql(loop))
