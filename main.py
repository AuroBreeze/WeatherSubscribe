import asyncio
import logging
import os.path

import pandas as pd
import re
import aiohttp
import yaml

from app.api import *
from app.switch import load_switch, save_switch
from app.config import *


list_group_id = []
json_data={"city_code": [],"QQ_number": []}
# 查看功能开关状态
def load_WeatherSubscribe_status(group_id):
    return load_switch(group_id, "WeatherSubscribe_status")


folder_path = "./data/WeatherSubscribe"
# 检查文件夹是否存在
if not os.path.exists(folder_path):
    # 如果文件夹不存在，则创建文件夹
    os.makedirs(folder_path)

# 数据存储路径，实际开发时，请将Example替换为具体的数据存放路径
DATA_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    "data",
    "WeatherSubscribe",
)
#-----------------------------------------------------------------------------------------------------------------------
class DB_WeatherSubscribe:
    def __init__(self, group_id=None):
        self.group_id = group_id

    def get_db_path(self):
        return os.path.join(
            os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
            "data",
            "WeatherSubscribe",
            f"{self.group_id}_WeatherSubscribe.db",
        )

    def db_init(self):
        db_path = self.get_db_path()
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

    def insert_people_to_db(self, user_id, city_code):
        people_data = [(user_id, city_code)]
        db_path = self.get_db_path()
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

    def delete_people_from_db(self, user_id):
        db_path = self.get_db_path()
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

    def find_people_in_db(self):
        db_path = self.get_db_path()
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # 执行查询，根据城市代码分组，并提取每个城市代码对应的所有QQ号
        cursor.execute("SELECT city_code, GROUP_CONCAT(qq_number) FROM WeatherSubscribe GROUP BY city_code")
        # 获取查询结果
        results = cursor.fetchall()

        # 关闭Cursor和Connection
        cursor.close()

        return results


# ----------------------------------------------------------------------------------------------------------------------

class Weather_Dectector:
    async def dict_find_classify_citycode(self, target_city):  # 词典寻找城市代码
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

    async def get_weather_data(self, citycode=370881):  # 获取天气数据
        key  = yaml.load(open("./scripts/WeatherSubscribe/config.yml", "r"), Loader=yaml.FullLoader)["WeatherAPI_KEY"]
        url = "https://restapi.amap.com/v3/weather/weatherInfo?key="+str(key)+f"&city={citycode}&extensions=base&output=json"
        # print(url)
        async with (aiohttp.ClientSession() as session):
            async with session.get(url) as response:
                if response.status == 200:
                    data = await response.text()
                    data = json.loads(data)


                    status = data["lives"][0]["weather"]
                    status_updata_time = data["lives"][0]["reporttime"]
                    windpower = data["lives"][0]["windpower"]
                    temperature = data["lives"][0]["temperature"]
                    ##print(status)

                    judgement_rain =self.get_rain_status(status)

                    if judgement_rain == True:

                        return True, status, status_updata_time, windpower, temperature
                    else:
                        return False
                else:
                    logging.error(f"获取天气数据失败，状态码：{response.status}")

    def get_rain_status(self,status):  # 判断是否下雨
        if "雨" in status or "雪" in status:
            return True
        else:
            return False

#-----------------------------------------------------------------------------------------------------------------------
class Handle_WeatherSubscribe():
    def __init__(self,websocket,msg):
        self.websocket = websocket
        self.user_id = str(msg.get("user_id"))
        self.group_id = str(msg.get("group_id"))
        self.raw_message = str(msg.get("raw_message"))
        self.name = str(msg.get("sender", {}).get("nickname"))

    async def handle_WeatherSubscribe_public_message(self):
        # 初始化数据库
        DB_WeatherSubscribe(self.group_id).db_init()

        try:
            if self.raw_message == "weather":
                content = (f"[CQ:at,qq={self.user_id}]\n"
                           f"使用方法：\n"
                           f"1. 发送“sub 城市名”订阅天气信息\n"
                           f"2. 发送“unpub”取消订阅天气信息\n"
                           f"暂时仅支持通知下雨天气。检查频率为15min/次。\n")
                await send_group_msg(self.websocket, self.group_id, content)
            else:
                pass

            if "sub" in self.raw_message:
                # 订阅天气
                msg_process = re.findall(r"sub (.*)", self.raw_message)

                if msg_process:
                    city_name = msg_process[0]
                    city_code = await Weather_Dectector().dict_find_classify_citycode(city_name)

                    if city_code == None:
                        content = f"[CQ:at,qq={self.user_id}] 未找到{city_name}的天气信息。"

                        await send_group_msg(self.websocket, self.group_id, content)
                    else:
                        content = f"[CQ:at,qq={self.user_id}] 已订阅{city_name}的天气信息。"

                        DB_WeatherSubscribe(self.group_id).insert_people_to_db(self.user_id, city_code)
                        await send_group_msg(self.websocket, self.group_id, content)

                        logging.info(f"{self.name}({self.user_id})订阅{city_name}的天气信息。")

            elif "unpub" in self.raw_message:
                # 取消订阅天气
                DB_WeatherSubscribe(self.group_id).delete_people_from_db(self.user_id)
                content = f"[CQ:at,qq={self.user_id}] 已取消订阅天气信息。"

                await send_group_msg(self.websocket, self.group_id, content)

                logging.info(f"{self.name}({self.user_id})取消订阅天气信息。")

            else:
                pass

        except Exception as e:
            logging.error(f"WeatherSubscribe_Error: {e}")

class Weather_Subscribe_sender:
    global list_group_id,json_data

    async def add_group_id(self):
        try:
            for filename in os.listdir(folder_path):
                if filename.endswith('.db'):  # 确保文件是.db格式
                    parts = filename.split('_')
                    if len(parts) > 1:
                        group_id = parts[0]
                        list_group_id.append(group_id)
                        logging.info(f"File: {filename}, Group ID: {group_id}")
                    else:
                        logging.info(f"File: {filename} does not contain a valid group ID")
        except Exception as e:
            logging.error(f"WeatherSubscribe_Error: {e}")


    async def send_weather_msg(self, websocket):

        try:
            for group_id in list_group_id:

                information_total = DB_WeatherSubscribe(group_id).find_people_in_db()
                for city_code, qq_number in information_total:
                    json_data["city_code"].append(city_code)
                    qq_list = qq_number.split(',')
                    json_data["QQ_number"].append(qq_list)

                num = 0
                for city_code in json_data["city_code"]:

                    weather_data, status, status_updata_time, windpower, temperature = await Weather_Dectector().get_weather_data(
                        city_code)
                    if weather_data == True:
                        # 遍历字典，为每个QQ号列表生成一个拼接字符串
                        qq_strings = []

                        qq_list = json_data['QQ_number'][num]
                        num += 1

                        # 使用列表推导式和join方法来构建每个QQ号字符串
                        qq_string = ''.join([f"[CQ:at,qq={qq}]" for qq in qq_list])
                        qq_strings.append(qq_string)

                        content = str(qq_strings[0]) + "\n" + (""
                                                               f"天气状况：{status}\n"
                                                               f"更新时间：{status_updata_time}\n"
                                                               f"风力：{windpower}\n"
                                                               f"温度：{temperature}\n")
                        await send_group_msg(websocket, group_id, content)
                    else:
                        pass

        except Exception as e:
            logging.error(f"WeatherSubscribe_Error: {e}")

        list_group_id.clear()
        json_data["QQ_number"].clear()
        json_data["city_code"].clear()

    async def handle_WeatherSubscribe_task_Start(self, websocket):
        await self.add_group_id()
        await self.send_weather_msg(websocket)

class Timer_count():

    def timer_count(self):
        now = datetime.now()
        #print(now)
        # 获取当前分钟
        current_minute = now.minute

        if current_minute % 15 ==0:
            return True
        else:
            return False
    async def handle_WeatherSubscribe_task_main(self, websocket):

        bool = self.timer_count()
        if bool == False:
            return
        await Weather_Subscribe_sender().add_group_id()
        task_sender = asyncio.create_task(Weather_Subscribe_sender().send_weather_msg(websocket))


async def handle_WeatherSubscribe_task_Msg(websocket,msg):
    msg_task = asyncio.create_task(Handle_WeatherSubscribe(websocket,msg).handle_WeatherSubscribe_public_message())

async def handle_WeatherSubscribe_task_Timer(websocket):
    timer_task = asyncio.create_task(Timer_count().handle_WeatherSubscribe_task_main(websocket))
