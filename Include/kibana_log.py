# -*- coding: utf-8 -*-
from elasticsearch import Elasticsearch
from Include.db.db import MySqlDb
import datetime

# ES实例化
es = Elasticsearch(
    '10.123.12.73:9200',
    # sniff_on_start=True,  # 连接前测试
    sniff_on_connection_fail=True,  # 节点无响应时刷新节点
    sniff_timeout=120  # 设置超时时间
)


def get_es_data(index, search):
    """获取ES数据"""
    resp = es.search(
        index=index,
        body={
            "size": 10000,
            "query": {
                "bool": {
                    "must": [
                        {
                            "query_string": {
                                "query": search,
                                "analyze_wildcard": True,
                                "default_field": "*"
                            }
                        }
                    ]
                }
            }
        }
    )
    return resp


# 服务集合
servers = [
    {
        'server_index': 'application-treasurerjob-*',
        'server_code': 'job'
    },
    {
        'server_index': 'application-treasurerservice*',
        'server_code': 'service'
    },
    {
        'server_index': 'application-treasurerview*',
        'server_code': 'view'
    }
]


def save_es_data():
    """保存ES数据"""
    mysqldb = MySqlDb()
    date = datetime.datetime.now()
    old_date = (date + datetime.timedelta(days=-1)).strftime("%Y-%m-%d") + '*'
    # old_date = '2021-01-24*'
    for server in servers:
        server_index = server['server_index']
        server_code = server['server_code']
        es_resp = get_es_data(server_index, 'Log-level:"ERROR" AND Log-time-tmp:"{}"'.format(old_date))
        hits = es_resp['hits']
        for hit_index in range(len(hits['hits'])):
            hit = hits['hits'][hit_index]
            time = hit['_source']['Log-time-tmp']
            level = hit['_source']['Log-level']
            message = hit['_source']['message']
            mysqldb.insert(
                "INSERT INTO kibana_log (server_code, log_tm, log_level, log_message) VALUES (%s, %s, %s, %s)",
                (server_code, time, level, message))
            mysqldb.mysql_db.commit()
            print("{}，{}第{}条错误日志，采集成功.".format(old_date, server_code, str(hit_index + 1)))
    print("全部采集完毕。")


if __name__ == '__main__':
    """主方法"""
    # 保存日志
    save_es_data()
