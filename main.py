import asyncio
import logging
import os.path

import pandas as pd
import re
import aiohttp
import yaml
import time
import datetime

from app.api import *
from app.switch import load_switch, save_switch
from app.config import *


list_group_id = []
json_data = {"city_code": [], "QQ_number": [], "weather_condition": [], "query_time": []}

used_list_qqnumber = []

bool_onoff = False


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

        # 创建一个表，包含新字段：weather_condition 和 query_time
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS WeatherSubscribe (
            qq_number TEXT PRIMARY KEY,
            city_code TEXT NOT NULL,
            weather_condition TEXT,
            query_time TEXT
        )
        ''')

        # 提交事务
        conn.commit()
        # 关闭Cursor
        cursor.close()
        # 关闭Connection
        conn.close()

    def insert_weather_info(self, user_id, city_code):
        db_path = self.get_db_path()
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # 如果没有提供天气情况和查询时间，则查询相同城市代码的最后一条记录的值

        last_info = self.get_last_weather_info(city_code)

        if last_info:
            weather_condition, query_time = last_info

        else:
            weather_condition = "晴"
            query_time = time.strftime("%Y-%m-%d", time.localtime())



        # 插入数据
        cursor.execute("INSERT OR REPLACE INTO WeatherSubscribe (qq_number, city_code, weather_condition, query_time) VALUES (?, ?, ?, ?)",
                       (user_id, city_code, weather_condition, query_time))

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

    def update_weather_info(self,weather_condition, query_time,qq_number):
        db_path = self.get_db_path()
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # 执行更新操作
        cursor.execute("UPDATE WeatherSubscribe SET weather_condition = ?, query_time = ? WHERE qq_number = ?", (weather_condition, query_time, qq_number))
        # 提交事务
        conn.commit()
        # 关闭Cursor
        cursor.close()
        # 关闭Connection
        conn.close()

    def get_last_weather_info(self, city_code):
        db_path = self.get_db_path()
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # 查询相同城市代码的最后一条记录的天气情况和查询时间
        cursor.execute("SELECT weather_condition, query_time FROM WeatherSubscribe WHERE city_code = ? ORDER BY qq_number DESC LIMIT 1", (city_code,))
        last_weather_info = cursor.fetchone()

        # 关闭Cursor和Connection
        cursor.close()
        conn.close()

        return last_weather_info

    def find_people_in_db(self):
        db_path = self.get_db_path()
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # 执行查询，根据城市代码分组，并提取每个城市代码对应的所有QQ号
        cursor.execute("SELECT city_code, GROUP_CONCAT(qq_number), GROUP_CONCAT(weather_condition), GROUP_CONCAT(query_time) FROM WeatherSubscribe GROUP BY city_code")
        # 获取查询结果
        results = cursor.fetchall()

        # 关闭Cursor和Connection
        cursor.close()
        conn.close()

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
        url = "https://restapi.amap.com/v3/weather/weatherInfo?key="+str(key)+f"&city={str(citycode)}&extensions=base&output=json"
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
                        return False,status,status_updata_time,windpower,temperature
                else:
                    logging.error(f"获取天气数据失败，状态码：{response.status}")

    def get_rain_status(self,status):  # 判断是否下雨
        if "雨" in status or "雪" in status:
            return True

#-----------------------------------------------------------------------------------------------------------------------
class Handle_WeatherSubscribe():
    def __init__(self,websocket,msg):
        self.websocket = websocket
        self.user_id = str(msg.get("user_id"))
        self.group_id = str(msg.get("group_id"))
        self.raw_message = str(msg.get("raw_message"))
        self.name = str(msg.get("sender", {}).get("nickname"))
        self.role = str(msg.get("sender", {}).get("role"))

    async def handle_WeatherSubscribe_public_message(self):
        global bool_onoff
        # 初始化数据库
        DB_WeatherSubscribe(self.group_id).db_init()

        try:
            if is_authorized(self.role, self.user_id): # 判断是否为管理员
                if self.raw_message == "subon":
                    bool_onoff = True
                    content = "天气订阅功能已启用。"
                    await send_group_msg(self.websocket, self.group_id, content)
                elif self.raw_message == "suboff":
                    bool_onoff = False
                    content = "天气订阅功能已禁用。"
                    await send_group_msg(self.websocket, self.group_id, content)
                else:
                    pass
            if "subon" == self.raw_message or "suboff" == self.raw_message:
                return  # 管理员命令不进行处理


            if self.raw_message == "weather":
                content = (f"[CQ:at,qq={self.user_id}]\n"
                           f"使用方法：\n"
                           f"1. 发送“sub 城市名”订阅天气信息\n"
                           f"2. 发送“unpub”取消订阅天气信息\n"
                           f"3. 发送“subon”启用天气订阅功能(admine)\n"
                           f"4. 发送“suboff”禁用天气订阅功能(admine)\n"
                           f"暂时仅支持通知下雨天气。检查频率为15min/次。\n")
                await send_group_msg(self.websocket, self.group_id, content)


            if bool_onoff == False: # 判断功能开关是否开启
                # content = "WeatherSubscribe功能未启用，请联系管理员。"
                # await send_group_msg(self.websocket, self.group_id, content)
                return

            if "file" in self.raw_message or "CQ" in self.raw_message: # 过滤文件消息
                return

            if "sub" in self.raw_message:
                # 订阅天气
                msg_process = re.findall(r"sub (.*)", self.raw_message)
                if msg_process == []:
                    content = "未检查到城市名称，请检查是否添加空格。"
                    await send_group_msg(self.websocket, self.group_id, content)
                    return

                if msg_process:
                    city_name = msg_process[0]
                    city_code = await Weather_Dectector().dict_find_classify_citycode(city_name)

                    if city_code == None:
                        content = f"[CQ:at,qq={self.user_id}] 未找到{city_name}的天气信息。"

                        await send_group_msg(self.websocket, self.group_id, content)
                    else:
                        content = f"[CQ:at,qq={self.user_id}] 已订阅{city_name}的天气信息。"

                        DB_WeatherSubscribe(self.group_id).insert_weather_info(self.user_id, city_code)
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
            logging.error(f"WeatherSubscribe_Error(Handle_WeatherSubscribe): {e}")

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
            logging.error(f"WeatherSubscribe_Error(Weather_Subscribe_sender.add_group_id): {e}")


    async def send_weather_msg(self, websocket):
        data = time.strftime("%Y-%m-%d", time.localtime())# 获得当前日期,并格式化为ISO格式

        try:
            #print(list_group_id)
            for group_id in list_group_id:
                information_total = DB_WeatherSubscribe(group_id).find_people_in_db()
                if information_total != []:  # 确保信息不为空
                    for city_code, qq_number, weather_condition, query_time in information_total:
                        json_data["city_code"].append(city_code)
                        qq_list = qq_number.split(',')
                        json_data["QQ_number"].append(qq_list)

                    num = 0
                    for city_code in json_data["city_code"]:

                        weather_data, status, status_updata_time, windpower, temperature = await Weather_Dectector().get_weather_data(
                            city_code)
                        #print(status)
                        if weather_data == True:
                            # 遍历字典，为每个QQ号列表生成一个拼接字符串
                            num_list = DB_WeatherSubscribe(group_id).get_last_weather_info(city_code)
                            #print(num_list)

                            if num_list[0] != "雨雪" or num_list[1] != data:
                                # 构建QQ号字符串列表
                                qq_strings = []
                                qq_list = json_data['QQ_number'][num]
                                #print(qq_list)

                                num += 1

                                # 使用列表推导式和join方法来构建每个QQ号字符串
                                qq_string = ''.join([f"[CQ:at,qq={qq}]" for qq in qq_list])


                                # 遍历查询到的天气信息，为每个QQ号添加天气信息

                                for qq in qq_list:
                                   DB_WeatherSubscribe(group_id).update_weather_info(weather_condition='雨雪',
                                                                                      query_time=data,
                                                                                      qq_number=qq)
                                qq_strings.append(qq_string)

                                content = str(qq_strings[0]) + "\n" + (""
                                                                   f"天气状况：{status}\n"
                                                                   f"更新时间：{status_updata_time}\n"
                                                                   f"风力：{windpower}\n"
                                                                   f"温度：{temperature}\n"
                                                                   f"城市：{city_code}\n")
                                await send_group_msg(websocket, group_id, content)

                        else:
                            qq_list = json_data['QQ_number'][num]
                            for qq in qq_list:
                                DB_WeatherSubscribe(group_id).update_weather_info(weather_condition=status,
                                                                                   query_time=data,
                                                                                   qq_number=qq)
                            num += 1
                            pass



        except Exception as e:
            logging.error(f"WeatherSubscribe_Error(Weather_Subscribe_sender.send_weather_msg): {e}")

        list_group_id.clear()
        json_data["QQ_number"].clear()  # 清空QQ号列表
        json_data["city_code"].clear()  # 清空城市代码列表

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
        global bool_onoff
        if bool_onoff == False:
            return
        bool = self.timer_count()
        if bool == False:
            return
        await Weather_Subscribe_sender().add_group_id()
        task_sender = asyncio.create_task(Weather_Subscribe_sender().send_weather_msg(websocket))


async def handle_WeatherSubscribe_task_Msg(websocket,msg):
    msg_task = asyncio.create_task(Handle_WeatherSubscribe(websocket,msg).handle_WeatherSubscribe_public_message())

async def handle_WeatherSubscribe_task_Timer(websocket):
    timer_task = asyncio.create_task(Timer_count().handle_WeatherSubscribe_task_main(websocket))
