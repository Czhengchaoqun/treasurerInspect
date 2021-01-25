# -*- coding: utf-8 -*-
"""
数据库操作
"""
import pymysql
import traceback


class MySqlDb:
    def __init__(self):
        try:
            db_host = '127.0.0.1'
            db_user = 'root'
            db_password = 'Server'
            db_database = 'treasurer_inspect'
            self.mysql_db = pymysql.connect(host=db_host, user=db_user, password=db_password, database=db_database, charset='utf8')
        except Exception as e:
            print('mysql异常-实例化' + traceback.format_exc())
            # logger_info.error('mysql异常-实例化' + traceback.format_exc())

    def query(self, sql, cursor=pymysql.cursors.DictCursor):
        """查询"""
        try:
            mysql_db = self.mysql_db
            # 创建游标对象（游标对象用于数据库执行，接收数据库返回结果）
            mysql_cursor = mysql_db.cursor(cursor)
            # 执行SQL语句
            mysql_cursor.execute(sql)
            # 获取全部结果集
            query_data = mysql_cursor.fetchall()
        except Exception as e:
            print('mysql异常-query' + traceback.format_exc())
            # logger_info.error('mysql异常-query' + traceback.format_exc())
        finally:
            # 关闭游标对象
            mysql_cursor.close()
        return query_data

    def insert(self, sql, val):
        """添加"""
        try:
            row_id = 0
            mysql_db = self.mysql_db
            # 创建游标对象（游标对象用于数据库执行，接收数据库返回结果）
            mysql_cursor = mysql_db.cursor(pymysql.cursors.DictCursor)
            # 执行SQL语句
            mysql_cursor.execute(sql, val)
            # 获取新增主键
            row_id = mysql_cursor.lastrowid
        except Exception as e:
            print('mysql异常-insert' + traceback.format_exc())
            # logger_info.error('mysql异常-insert' + traceback.format_exc())
            raise Exception('mysql异常-insert！')
        finally:
            # 关闭游标对象
            mysql_cursor.close()
        return row_id

    def insert_batch(self, sql, vals):
        """批量添加"""
        try:
            mysql_db = self.mysql_db
            # 创建游标对象（游标对象用于数据库执行，接收数据库返回结果）
            mysql_cursor = mysql_db.cursor(pymysql.cursors.DictCursor)
            # 执行SQL语句
            mysql_cursor.executemany(sql, vals)
        except Exception as e:
            print('mysql异常-insert_batch' + traceback.format_exc())
            # logger_info.error('mysql异常-insert_batch' + traceback.format_exc())
            raise Exception('mysql异常-insert_batch！')
        finally:
            # 关闭游标对象
            mysql_cursor.close()

    def delete(self, sql):
        """删除"""
        try:
            mysql_db = self.mysql_db
            # 创建游标对象（游标对象用于数据库执行，接收数据库返回结果）
            mysql_cursor = mysql_db.cursor(pymysql.cursors.DictCursor)
            # 执行SQL语句
            execute = mysql_cursor.execute(sql)
            return execute
        except Exception as e:
            print('mysql异常-delete' + traceback.format_exc())
            # logger_info.error('mysql异常-delete' + traceback.format_exc())
            raise Exception('mysql异常-delete！')
        finally:
            # 关闭游标对象
            mysql_cursor.close()

    def update(self, sql, val):
        """修改"""
        try:
            mysql_db = self.mysql_db
            # 创建游标对象（游标对象用于数据库执行，接收数据库返回结果）
            mysql_cursor = mysql_db.cursor(pymysql.cursors.DictCursor)
            # 执行sql语句
            execute = mysql_cursor.execute(sql, val)
            return execute
        except Exception as e:
            print('mysql异常-update' + traceback.format_exc())
            # logger_info.error('mysql异常-update' + traceback.format_exc())
            raise Exception('mysql异常-update！')
        finally:
            # 关闭游标对象
            mysql_cursor.close()


if __name__ == '__main__':
    MySqlDb().query("SELECT record_create_tm,log_tm,log_message FROM kibana_log WHERE is_push = '0'")
