import asyncio
import logging
import pandas as pd
import re
import aiohttp
import sqlite3
from datetime import datetime


from app.api import *
from app.switch import load_switch, save_switch
from app.config import *

bool_1 = True
bool_2 = False
json_data={
    "city_code": [],
    "QQ_number": []
}


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

def find_people_in_db(group_id):
    db_path = get_db_path(group_id)
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # 执行查询，根据城市代码分组，并提取每个城市代码对应的所有QQ号
    cursor.execute("SELECT city_code, GROUP_CONCAT(qq_number) FROM WeatherSubscribe GROUP BY city_code")
    # 获取查询结果
    results = cursor.fetchall()

    # 关闭Cursor和Connection
    cursor.close()

    return results


async def send_weather_msg(websocket,msg):
    global bool_1, bool_2
    while True:
        if bool_2 == True:
            break

        if bool_1 == True:
            bool_1 = False
            information_total = find_people_in_db(msg.get("group_id"))
            for city_code, qq_number in information_total:
                #print(f"城市代码：{city_code}，对应的QQ号：{qq_number}")
                json_data["city_code"].append(city_code)
                qq_list = qq_number.split(',')
                json_data["QQ_number"].append(qq_list)

            num=0
            for city_code in json_data["city_code"]:

                weather_data,status,status_updata_time,windpower,temperature = await get_weather_data(city_code)
                if weather_data == True:
                    # 遍历字典，为每个QQ号列表生成一个拼接字符串
                    qq_strings = []
                    for qq_list in json_data['QQ_number']:
                        # 使用列表推导式和join方法来构建每个QQ号字符串
                        qq_string = ''.join([f"[CQ:at,qq={qq}]" for qq in qq_list])
                        qq_strings.append(qq_string)
                    content = str(qq_strings[0]) +"\n"+(""
                                                        f"天气状况：{status}\n"
                                                        f"更新时间：{status_updata_time}\n"
                                                        f"风力：{windpower}\n"
                                                        f"温度：{temperature}\n")
                    await send_group_msg(websocket, msg.get("group_id"), content)
                else:
                    pass

            await asyncio.sleep(900)

            bool_1 = True
            bool_2 = True

            pass
        else:
            break


async def get_weather_data(citycode):  # 获取天气数据
    url = f"https://www.haotechs.cn/ljh-wx/weather?adcode={citycode}"
    #print(url)
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            if response.status == 200:
                data = await response.text()
                data = json.loads(data)

                status = data["result"]["weather"]
                status_updata_time = data["result"]["reporttime"]
                windpower = data["result"]["windpower"]
                temperature = data["result"]["temperature"]
                ##print(status)

                judgement_rain = get_rain_status(status)

                if judgement_rain == True:


                    return True, status, status_updata_time, windpower, temperature
                else:
                    return False
            else:
                logging.error(f"获取天气数据失败，状态码：{response.status}")

def get_rain_status(status):  # 判断是否下雨
    if "雨" in status:
        return True
    else:
        return False

async def dict_find_classify_citycode(target_city):  # 词典寻找城市代码
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


async def handle_WeatherSubscribe_public_message(websocket,msg):
    user_id = str(msg.get("user_id"))
    group_id = str(msg.get("group_id"))
    raw_message = str(msg.get("raw_message"))
    name = str(msg.get("sender", {}).get("nickname"))
    #message_id = str(msg.get("message_id"))

    # 初始化数据库
    db_init(group_id)

    try:
        if raw_message == "weather":
            content = (f"[CQ:at,qq={user_id}]\n"
                       f"使用方法：\n"
                       f"1. 发送“sub 城市名”订阅天气信息\n"
                       f"2. 发送“unpub”取消订阅天气信息\n"
                       f"暂时仅支持通知下雨天气。检查频率为15min/次。\n")
            await send_group_msg(websocket, group_id, content)
        else:
            pass


        if "sub" in raw_message:
            # 订阅天气
            msg_process = re.findall(r"sub (.*)", raw_message)

            if msg_process:
                city_name = msg_process[0]
                city_code = await dict_find_classify_citycode(city_name)

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
            pass

    except Exception as e:
        logging.error(f"WeatherSubscribe_Error: {e}")


async def handle_WeatherSubscribe_task(websocket,msg):

    msg_task = asyncio.create_task(handle_WeatherSubscribe_public_message(websocket,msg))
    send_msg_task = asyncio.create_task(send_weather_msg(websocket,msg))
