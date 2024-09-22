import asyncio
import logging
import pandas as pd
import re
import aiohttp
import sqlite3


from app.config import owner_id
from app.api import *
from app.switch import load_switch, save_switch

# 数据存储路径，实际开发时，请将Example替换为具体的数据存放路径
DATA_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    "data",
    "WeatherSubscribe",
)

# 查看功能开关状态
def load_WeatherSubscribe_status(group_id):
    return load_switch(group_id, "WeatherSubscribe_status")

def get_db_path(group_id):
    return os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
        "data",
        "WeatherSubscribe",
        f"{group_id}_WeatherSubscribe.db",
    )

def db_init(group_id):
    db_path = get_db_path(group_id)
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # 创建一个表
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS WeatherSubscribe (
        qq_number TEXT PRIMARY KEY,
        city_code TEXT NOT NULL
    )
    ''')

    # 提交事务
    conn.commit()
    # 关闭Cursor
    cursor.close()
    # 关闭Connection
    conn.close()

def insert_people_to_db(group_id,user_id, city_code):
    people_data = [(user_id, city_code)]
    db_path = get_db_path(group_id)
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # 插入数据
    cursor.executemany("INSERT OR REPLACE INTO WeatherSubscribe (qq_number,city_code) VALUES (?,?)", people_data)

    # 提交事务
    conn.commit()
    # 关闭Cursor
    cursor.close()
    # 关闭Connection
    conn.close()

def delete_people_from_db(group_id,user_id):
    db_path = get_db_path(group_id)
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # 执行删除操作
    cursor.execute("DELETE FROM WeatherSubscribe WHERE qq_number = ?", (user_id,))

    # 提交事务
    conn.commit()
    # 关闭Cursor
    cursor.close()
    # 关闭Connection
    conn.close()

async def get_weather_data(citycode):  # 获取天气数据
    url = f"https://www.haotechs.cn/ljh-wx/weather?adcode={citycode}"
    #print(url)
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            if response.status == 200:
                data = await response.text()
                data = json.loads(data)
                return data
            else:
                return None


async def handle_WeatherSubscribe_public_message(websocket,msg):
    user_id = str(msg.get("user_id"))
    group_id = str(msg.get("group_id"))
    raw_message = str(msg.get("raw_message"))
    name = str(msg.get("sender", {}).get("nickname"))
    #message_id = str(msg.get("message_id"))

    # 初始化数据库
    db_init(group_id)

    try:
        if "sub" in raw_message:
            # 订阅天气
            msg_process = re.findall(r"sub (.*)", raw_message)

            if msg_process:
                city_name = msg_process[0]
                city_code = await dict_find_citycode(city_name)

                if city_code == None:
                    content = f"[CQ:at,qq={user_id}] 未找到{city_name}的天气信息。"

                    await send_group_msg(websocket, group_id, content)
                else:
                    content = f"[CQ:at,qq={user_id}] 已订阅{city_name}的天气信息。"

                    insert_people_to_db(group_id, user_id, city_code)
                    await send_group_msg(websocket, group_id, content)

                    logging.info(f"{name}({user_id})订阅{city_name}的天气信息。")

        elif "unpub" in raw_message:
            # 取消订阅天气
            delete_people_from_db(group_id,user_id)
            content = f"[CQ:at,qq={user_id}] 已取消订阅天气信息。"

            await send_group_msg(websocket, group_id, content)

            logging.info(f"{name}({user_id})取消订阅天气信息。")

        else:
            content = f"[CQ:at,qq={user_id}] 指令错误，请使用sub或unpub来进行订阅或取消订阅。"

            await send_group_msg(websocket, group_id, content)

    except Exception as e:
        pass
        # content = f"[CQ:at,qq={user_id}] 天气订阅功能出错，请检查输入是否正确。"
        # await send_group_msg(websocket, user_id, content)

async def dict_find_citycode(target_city):  # 词典寻找城市代码
    # 读取Excel文件
    df = pd.read_csv('./scripts/WeatherSubscribe/citycode.csv', encoding='utf-8')
    # 假设我们要在名为'column_name'的列中查找包含特定文字的单元格
    # 并且我们想要获取该列后面一列的数据
    column_name = 'name'  # 替换为你的列名
    next_column_name = df.columns[df.columns.get_loc(column_name) + 1]  # 获取后面一列的列名
    # 找到包含特定文字的行
    filtered_df = df[df[column_name].str.contains(target_city, na=False)]
    # 获取后面一列的数据
    city_code = filtered_df[next_column_name].tolist()

    if len(city_code) == 0:
        city_code = None
    else:
        city_code = city_code[0]
    return city_code

