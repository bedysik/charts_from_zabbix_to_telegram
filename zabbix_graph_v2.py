import datetime
import json
import os
import requests
import re
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup
from aiogram.utils import executor
from pyzabbix import ZabbixAPI
import logging
from aiogram import Bot, Dispatcher, types
import sqlite3

import config


# задаем уровень логов
logging.basicConfig(level=logging.INFO)

# инициализируем бота
bot = Bot(token=config.BotToken)
dp = Dispatcher(bot)

message_id_del = []


def sql_connection():
    config.DbConn = sqlite3.connect(config.DbName)


async def sql_create_db():
    # deleting the old zabbix_object.db database on a new bot start
    await bot.send_message(config.ChatSend, "Выполняю обновление базы")

    if os.path.isfile(config.DbName):
        os.remove(config.DbName)

    conn = sqlite3.connect(config.DbName)
    cur = conn.cursor()
    cur.execute("""CREATE TABLE IF NOT EXISTS testobjects(
               host_id INT PRIMARY KEY,
               host_name TEXT,
               host_group TEXT,
               graph TEXT,
               graph_name TEXT);
            """)
    cur.execute("""CREATE TABLE IF NOT EXISTS ppakobjects(
                   host_id INT PRIMARY KEY,
                   host_name TEXT,
                   host_group TEXT,
                   graph TEXT,
                   graph_name TEXT);
                """)
    cur.execute("""CREATE TABLE IF NOT EXISTS users(
                   user_id TEXT,
                   user_name TEXT,
                   stand TEXT,
                   host_group TEXT,
                   host_id TEXT,
                   host_name TEXT,
                   graph_id TEXT,
                   graph_name TEXT,
                   time TEXT,
                   host_list TEXT,
                   graph_list TEXT);
                """)
    conn.commit()

    for k in config.UrlApiZabbix:
        list_sql_test = []
        temp_stand = k
        auth_api = ZabbixAPI(config.UrlApiZabbix[k])
        auth_api.login(config.ZabbUser, config.ZabbPass)
        host_list = auth_api.host.get()
        for i in host_list:
            if i["status"] == "0":
                list_temp = []
                list_graph_name = []
                dict_temp = {}
                host_id = i["hostid"]
                host_name = str(i["name"])
                list_temp.append(host_id)
                list_temp.append(host_name)
                if re.search(r'.*(proddb).*', host_name) != None or re.search(r'.*(ora).*', host_name) != None or re.search(r'.*(pg).*', host_name) != None:
                    list_temp.append("database")
                elif re.search(r'.*(erpp).*', host_name) != None or re.search(r'.*(erp-app).*', host_name) != None or re.search(r'.*(erp-proda).*', host_name) != None or re.search(r'.*(srv-erp-ksn03).*', host_name) != None or re.search(r'.*(srv-erp-tst01).*', host_name) != None:
                    list_temp.append("erp")
                elif re.search(r'.*(school).*', host_name) != None:
                    list_temp.append("school")
                elif re.search(r'.*(stud).*', host_name) != None:
                    list_temp.append("student")
                else:
                    list_temp.append("other")

                graph_id = auth_api.graph.get(hostids=host_id)
                for k in graph_id:
                    graph_id = k["graphid"]
                    graph_name = k["name"]
                    dict_temp[graph_name] = graph_id
                    list_graph_name.append(graph_name)
                dict_temp_string = ', '.join(['"{0}":"{1}"'.format(key, value) for (key, value) in dict_temp.items()])
                list_graph_name_string = ",".join(list_graph_name)
                list_temp.append(dict_temp_string)
                list_temp.append(list_graph_name_string)
                list_sql_test.append(list_temp)
        for u in list_sql_test:
            cur.executemany("INSERT INTO " + "'" + temp_stand + "objects" + "'" + " VALUES(?, ?, ?, ?, ?);", (u,))
            conn.commit()

    conn.close()

    await bot.send_message(config.ChatSend, "Готов к работе")
    await startup_keyboard()


async def sql_requests(request):
    try:
        cur = config.DbConn.cursor()
        cur.execute(request)
        sql_result = cur.fetchall()
        return sql_result
    except AttributeError:
        sql_connection()
        cur = config.DbConn.cursor()
        cur.execute(request)
        sql_result = cur.fetchall()
        return sql_result


async def sql_updates(update):
    try:
        cur = config.DbConn.cursor()
        cur.execute(update)
        config.DbConn.commit()
    except AttributeError:
        sql_connection()
        cur = config.DbConn.cursor()
        cur.execute(update)
        config.DbConn.commit()


async def sql_inserts(insert_list, insert_request):
    conn = sqlite3.connect(config.DbName)
    cur = conn.cursor()
    insert_result = cur.execute(insert_request, insert_list)
    conn.commit()
    conn.close()
    return insert_result



async def del_message():
    global message_id_del
    for i in message_id_del:
        await bot.delete_message(config.ChatSend, i.message_id)


async def startup_keyboard():
    global message_id_del
    message_id_del.clear()
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True)
    item1 = types.KeyboardButton("TEST")
    item2 = types.KeyboardButton("PPAK")
    markup.add(item1)
    markup.add(item2)

    temp_id = await bot.send_message(config.ChatSend, 'Выберите стенд', reply_markup=markup)
    message_id_del.append(temp_id)


async def get_host_group():
    global message_id_del
    message_id_del.clear()
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True)
    item1 = types.KeyboardButton("erp")
    item2 = types.KeyboardButton("school")
    item3 = types.KeyboardButton("student")
    item4 = types.KeyboardButton("database")
    item5 = types.KeyboardButton("other")
    markup.add(item1)
    markup.add(item2)
    markup.add(item3)
    markup.add(item4)
    markup.add(item5)

    temp_id = await bot.send_message(config.ChatSend, 'Выберите группу', reply_markup=markup)
    message_id_del.append(temp_id)


async def get_hostid_view(message):
    global message_id_del
    request = "SELECT stand, host_group FROM users WHERE user_id = '" + str(message.from_user.id) + "';"

    sql_result = await sql_requests(request)

    request = "SELECT host_name FROM " + (sql_result[0])[0] + "objects WHERE host_group = '" + (sql_result[0])[1] + "';"

    sql_result = await sql_requests(request)

    temp_list = []
    for i in sql_result:
        temp_list.append(i[0])

    temp_list.sort()
    update = "UPDATE users SET host_list = '" + ",".join(temp_list) + "' WHERE user_id = '" + str(message.from_user.id) + "';"

    await sql_updates(update)

    buttons_list = []
    for item in temp_list:
        buttons_list.append([InlineKeyboardButton(text=item, callback_data=item)])
    keyboard_inline_buttons = InlineKeyboardMarkup(inline_keyboard=buttons_list, row_width=2)

    temp_id = await bot.send_message(chat_id=config.ChatSend, text="Выберите название хоста",
                                     reply_markup=keyboard_inline_buttons)
    message_id_del.append(temp_id)


# Нахождение нужного id хоста на осноме выбраного имени пользователем
async def get_hostid(call):
    global message_id_del
    request = "SELECT stand, host_name FROM users WHERE user_id = '" + str(call.from_user.id) + "';"

    sql_result = await sql_requests(request)

    request = "SELECT host_id FROM " + (sql_result[0])[0] + "objects WHERE host_name = '" + (sql_result[0])[1] + "';"

    sql_result = await sql_requests(request)

    host_id = str((sql_result[0])[0])
    update = "UPDATE users SET host_id = '" + host_id + "' WHERE user_id = '" + str(call.from_user.id) + "';"

    await sql_updates(update)

    temp_id = await bot.send_message(config.ChatSend, 'Выберите название графика')
    message_id_del.append(temp_id)

    await get_graphid_view(call)


# функция списка графиков для выбора
async def get_graphid_view(call):
    global message_id_del
    request = "SELECT stand, host_id FROM users WHERE user_id = '" + str(call.from_user.id) + "';"

    sql_result = await sql_requests(request)

    request = "SELECT graph FROM " + (sql_result[0])[0] + "objects WHERE host_id = '" + (sql_result[0])[1] + "';"

    sql_result = await sql_requests(request)

    update = "UPDATE users SET graph_list = '" + (sql_result[0])[0] + "' WHERE user_id = '" + str(call.from_user.id) + "';"

    await sql_updates(update)

    temp_graph_dict = json.loads("{" + ((sql_result[0])[0]) + "}")
    buttons_list = []
    for item in temp_graph_dict:
        buttons_list.append([InlineKeyboardButton(text=item, callback_data=str(temp_graph_dict[item]))])

    if len(buttons_list) < 90:
        keyboard_inline_buttons = InlineKeyboardMarkup(inline_keyboard=buttons_list, row_width=3)

        temp_id = await bot.send_message(chat_id=config.ChatSend, text="Выберите график", reply_markup=keyboard_inline_buttons)
        message_id_del.append(temp_id)
    else:
        buttons_list_1 = buttons_list[:90]
        buttons_list_2 = buttons_list[90:]
        keyboard_inline_buttons_1 = InlineKeyboardMarkup(inline_keyboard=buttons_list_1, row_width=3)

        temp_id = await bot.send_message(chat_id=config.ChatSend, text="Выберите график",  reply_markup=keyboard_inline_buttons_1)
        message_id_del.append(temp_id)
        keyboard_inline_buttons_2 = InlineKeyboardMarkup(inline_keyboard=buttons_list_2, row_width=3)

        temp_id = await bot.send_message(chat_id=config.ChatSend, text="Выберите график", reply_markup=keyboard_inline_buttons_2)
        message_id_del.append(temp_id)


async def get_graphid(call, temp_graph_name):
    global message_id_del
    update = "UPDATE users SET graph_name = '" + temp_graph_name + "' WHERE user_id = '" + str(call.from_user.id) + "';"

    await sql_updates(update)

    markup = types.ReplyKeyboardMarkup(resize_keyboard=True)
    item1 = types.KeyboardButton("last 1h")
    markup.add(item1)

    temp_id = await bot.send_message(config.ChatSend,
                                     'Выберите время\nлибо введите в формате (2022-05-20 14:00_2022-05-20 16:00)',
                                     reply_markup=markup)
    message_id_del.append(temp_id)


async def get_time_graph(message):
    request = "SELECT time FROM users WHERE user_id = '" + str(message.from_user.id) + "';"

    sql_result = await sql_requests(request)

    # если пользователь ввел last 1h
    if (sql_result[0])[0] == "last 1h":
        time_graph_url = ("now-1h&to=now")

        await get_url_graph(message, time_graph_url)

    else:
        temp_date_1 = datetime.datetime.strptime(((sql_result[0])[0])[:16], '%Y-%m-%d %H:%M')
        temp_date_2 = datetime.datetime.strptime(((sql_result[0])[0])[17:], '%Y-%m-%d %H:%M')
        start_date = ((sql_result[0])[0])[:4] + ((sql_result[0])[0])[5:7] + ((sql_result[0])[0])[8:10] + ((sql_result[0])[0])[11:13] + ((sql_result[0])[0])[14:16] + "00"
        total_seconds = (temp_date_2 - temp_date_1).seconds
        time_graph_url = "period=" + str(total_seconds) + "&stime=" + start_date + "&isNow=0"

        await get_url_graph(message, time_graph_url)


async def get_url_graph(message, time_graph_url):
    graph_url_send = "0"
    request = "SELECT stand, graph_id FROM users WHERE user_id = '" + str(message.from_user.id) + "';"

    sql_result = await sql_requests(request)

    temp_url_stand = "0"
    if (sql_result[0])[0] == "test":
        temp_url_stand = config.UrlTest
    elif (sql_result[0])[0] == "ppak":
        temp_url_stand = config.UrlPpak
    if time_graph_url == "now-1h&to=now":
        graph_url_send = (temp_url_stand + "/chart2.php?graphid=" + str((sql_result[0])[1]) + "&from=" + str(time_graph_url) + "&profileIdx=web.graphs.filter&profileIdx2=2007&width=1782&height=201&_=v3veuwkm&screenid=.png")
    elif time_graph_url != "now-1h&to=now":
        graph_url_send = (temp_url_stand + "/chart2.php?graphid=" + str((sql_result[0])[1]) + "&" + str(time_graph_url) + "&profileIdx=web.graphs.filter&profileIdx2=2007&width=1782&height=201&_=v3veuwkm&screenid=.png")

    await get_graph_send(message, temp_url_stand, graph_url_send)


async def get_graph_send(message, temp_url_stand, graph_url_send):
    request = "SELECT stand, host_name, graph_name, time FROM users WHERE user_id = '" + str(message.from_user.id) + "';"

    sql_result = await sql_requests(request)

    text_message_graph = (sql_result[0])[0] + "\n" + (sql_result[0])[1] + "\n" + (sql_result[0])[2] + "\n" + (sql_result[0])[3]
    # сессия для сохранения изображения графика
    session = requests.Session()
    session.post(temp_url_stand + '/index.php',
                 {'name': config.ZabbUser, 'password': config.ZabbPass, 'autologin': '1', 'enter': 'Sign in'})
    response = session.get(graph_url_send)
    response.raise_for_status()
    # формирование и сохрание изображения графика
    with open(config.GraphName, 'wb') as file:
        file.write(response.content)

    await del_message()
    await bot.send_photo(config.ChatSend, open(config.GraphName, "rb"), caption=text_message_graph)
    await startup_keyboard()


@dp.message_handler(commands=['update_db'])
async def start_message(message: types.Message):
    await sql_create_db()


@dp.message_handler(commands=['start'])
async def status_message(message: types.Message):
    global message_id_del
    message_id_del.clear()

    await bot.delete_message(config.ChatSend, message.message_id)
    await startup_keyboard()


@dp.message_handler(content_types=['text'])
async def start_new_operation(message: types.Message):
    global message_id_del
    if message.text == "TEST":
        stand = 'test'
        request = "SELECT user_id, user_name FROM users WHERE user_id = '" + str(message.from_user.id) + "';"

        sql_result = await sql_requests(request)

        if len(sql_result):
            if (sql_result[0])[0] == str(message.from_user.id):
                update = "UPDATE users SET stand = '" + stand + "' WHERE user_id = '" + str(message.from_user.id) + "';"

                await sql_updates(update)

        else:
            insert_request = "INSERT INTO 'users' VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);"
            insert_list = []
            temp_list = ["0", "0", "0", "0", "0", "0", "0", "0"]
            insert_list.append(message.from_user.id)
            insert_list.append(message.from_user.first_name)
            insert_list.append(stand)
            insert_list.extend(temp_list)

            await sql_inserts(insert_list, insert_request)

        message_id_del.append(message)

        await get_host_group()

    elif message.text == "PPAK":
        stand = 'ppak'
        request = "SELECT user_id, user_name FROM users WHERE user_id = '" + str(message.from_user.id) + "';"

        sql_result = await sql_requests(request)

        if len(sql_result):
            if (sql_result[0])[0] == str(message.from_user.id):
                update = "UPDATE users SET stand = '" + stand + "' WHERE user_id = '" + str(message.from_user.id) + "';"

                await sql_updates(update)

        else:
            insert_request = "INSERT INTO 'users' VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);"
            insert_list = []
            temp_list = ["0", "0", "0", "0", "0", "0", "0", "0"]
            insert_list.append(message.from_user.id)
            insert_list.append(message.from_user.first_name)
            insert_list.append(stand)
            insert_list.extend(temp_list)

            await sql_inserts(insert_list, insert_request)

        message_id_del.append(message)

        await get_host_group()

    elif message.text in config.HostGroup:
        host_group = message.text
        update = "UPDATE users SET host_group = '" + host_group + "' WHERE user_id = '" + str(message.from_user.id) + "';"
        await sql_updates(update)

        message_id_del.append(message)

        await get_hostid_view(message)

    elif message.text == "last 1h":
        update = "UPDATE users SET time = '" + message.text + "' WHERE user_id = '" + str(message.from_user.id) + "';"

        await sql_updates(update)

        message_id_del.append(message)

        await get_time_graph(message)

    elif message.text != "last 1h" and message.text != "TEST" and message.text != "PPAK" and message.text != "Выбрать график":
        regex_text = re.match(r'(\d{4}\-\d{2}\-\d{2}\s\d{2}\:\d{2}\_\d{4}\-\d{2}\-\d{2}\s\d{2}\:\d{2})', message.text)
        result_check = regex_text[0] if regex_text else 'Not found'
        if result_check != 'Not found' and regex_text[0] == message.text:
            update = "UPDATE users SET time = '" + message.text + "' WHERE user_id = '" + str(message.from_user.id) + "';"

            await sql_updates(update)

            message_id_del.append(message)

            await get_time_graph(message)

    else:
        await bot.send_message(config.ChatSend, "Неверная команда")


@dp.callback_query_handler()
async def define_user_text(call):
    request = "SELECT host_list FROM users WHERE user_id = '" + str(call.from_user.id) + "';"

    sql_result = await sql_requests(request)

    temp_host_list = (sql_result[0])[0].split(",")
    if call.data in temp_host_list:
        update = "UPDATE users SET host_name = '" + call.data + "' WHERE user_id = '" + str(call.from_user.id) + "';"

        await sql_updates(update)

        await get_hostid(call)

    else:
        request = "SELECT graph_list FROM users WHERE user_id = '" + str(call.from_user.id) + "';"

        sql_result = await sql_requests(request)

        temp_graph_dict = json.loads("{" + ((sql_result[0])[0]) + "}")
        for temp_graph_name in temp_graph_dict:
            if temp_graph_dict[temp_graph_name] == call.data:
                update = "UPDATE users SET graph_id = '" + call.data + "' WHERE user_id = '" + str(call.from_user.id) + "';"

                await sql_updates(update)

                await get_graphid(call, temp_graph_name)



if __name__ == '__main__':
    await sql_create_db()
    executor.start_polling(dp, skip_updates=True)
