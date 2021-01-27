# -*- coding: utf-8 -*-
import requests
import json
from selenium import webdriver
from Include.db.db import MySqlDb
import traceback
import time
from Include.util.base_util import *
from socket import *

"""应用信息"""
app_info = {
    'app_id': 'cli_9f6623d3a1bcd00d',
    'app_secret': 'ykxxyunu5VVSxBRoo653VfA2hczR4g56'
}

"""页签信息"""
sheet_info = {
    'job': {
        'id': 'e8e68c',
        'code': 'job'
    },
    'service': {
        'id': 'EqBxNj',
        'code': 'service'
    },
    'view': {
        'id': 'T9ovmN',
        'code': 'view'
    },
}


def get_app_access_token():
    """获取应用访问TOKEN"""
    get_app_access_token_url = 'https://open.feishu.cn/open-apis/auth/v3/app_access_token/internal/'
    res = requests.post(url=get_app_access_token_url, json=app_info)
    res_json = json.loads(res.text)
    return res_json['app_access_token']


def get_login_code():
    """获取登陆CODE"""
    req_time = int(round(time.time() * 1000000))

    # 打开浏览器登陆
    get_login_code_url = 'https://open.feishu.cn/open-apis/authen/v1/index?redirect_uri=http%3A%2F%2F127.0.0.1%3A5001%2F&app_id={}&state={}'.format(
        app_info.get('app_id'), req_time)
    # 打开火狐浏览器
    driver = webdriver.Firefox()
    driver.maximize_window()
    driver.get(get_login_code_url)

    # 登陆后CODE
    res_code = ''

    # 发布地址接收登陆后重定向返回
    # 创建服务器端套接字对象
    server_socket = socket(AF_INET, SOCK_STREAM)
    # 绑定端口
    server_socket.bind(("127.0.0.1", 5001))
    # 监听
    server_socket.listen()
    # 多次通信
    client_sock, addr = server_socket.accept()
    while True:
        data = client_sock.recv(1024)
        data_len = len(data)
        if data_len > 0:
            data = data.decode()
            first_line = data.splitlines()[0]
            code = get_str_btw(first_line, 'code=', '&')
            # state = get_str_btw(first_line, 'state=', ' HTTP')
            res_code = code
            client_sock.send("登陆成功，CODE采集成功，请关闭页面！".encode('gbk'))
            break
    client_sock.close()
    server_socket.close()

    # 关闭火狐浏览器
    driver.quit()
    return res_code


def get_user_access_token():
    """获取用户访问TOKEN"""
    get_user_access_token_url = 'https://open.feishu.cn/open-apis/authen/v1/access_token'
    get_user_access_token_json = {
        'app_access_token': get_app_access_token(),
        'grant_type': 'authorization_code',
        'code': get_login_code()
    }
    res = requests.post(url=get_user_access_token_url, json=get_user_access_token_json)
    res_json = json.loads(res.text)
    res_json_code = res_json['code']
    if res_json_code == 0:
        access_token = res_json['data']['access_token']
    else:
        print('获取用户访问TOKEN失败')
    return access_token


def additional_data(sheet, query_time, user_access_token):
    """向EXCEL表格追加数据"""
    # 服务编码
    server_code = sheet['code']
    # 页签主键
    sheet_id = sheet['id']
    # 追加数据API接口地址
    additional_data_url = 'https://open.feishu.cn/open-apis/sheet/v2/spreadsheets/shtcnCdbQoSGJoRj6aGQNoYV4pg/values_append'
    # 追加数据API接口HEADER
    additional_data_headers = {'Authorization': 'Bearer {}'.format(user_access_token)}

    # 截取字符串结束位置
    substring_end_index = 700

    # 追加数据
    mysqldb = MySqlDb()
    query_log_len_sql = "SELECT COUNT(1) AS sum_length FROM (SELECT COUNT(1) AS length FROM kibana_log WHERE is_push = '0' AND server_code = '{SERVER_CODE}' AND record_create_tm LIKE '{QUERY_TIME}%' GROUP BY SUBSTRING(log_message, LENGTH(substring_index(log_message, '|', 6))+2, {SUBSTRING_END_INDEX})) AS group_table" \
        .format(SERVER_CODE=server_code, QUERY_TIME=query_time, SUBSTRING_END_INDEX=substring_end_index)
    log_len = mysqldb.query(sql=query_log_len_sql)[0]['sum_length']
    max_length = 1000
    if log_len != 0:
        page_size = int(log_len / max_length) + 1
        for i in range(page_size):
            try:
                query_log_sql = "SELECT id,record_create_tm,log_tm,log_message,GROUP_CONCAT(id) AS sum_id FROM kibana_log WHERE is_push = '0' AND server_code = '{SERVER_CODE}' AND record_create_tm LIKE '{QUERY_TIME}%' GROUP BY SUBSTRING(log_message, LENGTH(substring_index(log_message, '|', 6))+2, {SUBSTRING_END_INDEX}) ORDER BY log_tm ASC LIMIT {MAX_LENGTH}" \
                    .format(SERVER_CODE=server_code, QUERY_TIME=query_time, MAX_LENGTH=max_length,
                            SUBSTRING_END_INDEX=substring_end_index)
                log_data = mysqldb.query(sql=query_log_sql)
                add_data = list()
                add_id = list()
                for log_item_data in log_data:
                    log_id = log_item_data['id']
                    log_sum_id = log_item_data['sum_id']
                    record_create_tm = log_item_data['record_create_tm']
                    log_tm = log_item_data['log_tm']
                    log_message = log_item_data['log_message']
                    record_create_tm = record_create_tm.strftime("%Y-%m-%d %H:%M:%S")
                    log_tm = log_tm.strftime("%Y-%m-%d %H:%M:%S")
                    max_bytes = 48000
                    log_message = [log_message[i:i + max_bytes] for i in range(0, len(log_message), max_bytes)][0]
                    add_data.append([log_id, record_create_tm, log_tm, log_message])
                    add_id.extend(log_sum_id.split(','))
                req_data = {
                    'valueRange': {
                        'range': '{}!A2:D5000'.format(sheet_id),
                        'values': add_data
                    }
                }
                req_json = json.dumps(obj=req_data)
                res = requests.post(url=additional_data_url, headers=additional_data_headers, data=req_json)
                res_json = json.loads(res.text)
                if res_json['code'] == 0:
                    update_log_push_sql = "UPDATE kibana_log SET is_push = '1' WHERE id in ({})" \
                        .format(','.join(['%s' for a in range(len(add_id))]))
                    mysqldb.update(sql=update_log_push_sql, val=add_id)
                    mysqldb.mysql_db.commit()
                    print('{}服务，第{}批同步共享Excel，同步数量{}，同步成功。'.format(server_code, i, len(add_data)))
                else:
                    raise Exception('同步共享Excel时失败，原因：{}'.format(res_json['msg']))
            except Exception as e:
                print('{}服务，第{}批同步共享Excel，同步数量{}，同步失败，异常：{}'.format(server_code, i, len(add_data),
                                                                    traceback.format_exc()))
    else:
        print('{}服务，无日志需要同步'.format(server_code))
    mysqldb.mysql_db.close()


if __name__ == '__main__':
    """主方法"""
    user_access_token = get_user_access_token()
    # user_access_token = 'u-I9HQoL01HpshHoSbRdDjNh'
    print('user_access_token：{}'.format(user_access_token))
    if user_access_token is not None and user_access_token != '':
        time = time.strftime('%Y-%m-%d')
        # time = '2021-01-25'
        # job页签数据推送
        additional_data(sheet_info['job'], time, user_access_token)
        # service页签数据推送
        additional_data(sheet_info['service'], time, user_access_token)
        # view页签数据推送
        additional_data(sheet_info['view'], time, user_access_token)
