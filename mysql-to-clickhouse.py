#!/usr/bin/env python3
# encoding: utf-8
"""
@by: wenlongy
@contact: 247540299@qq.com
@PROJECT_NAME: mysql-to-clickhouse
@file: mysql-to-clickhouse.py
@time: 2020-04-07 19:39
"""

import copy
import redis
import time
import pymysql
import datetime
from tools import *
# clockhouse
from clickhouse_driver import Client
# binlog
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import (
    DeleteRowsEvent,
    UpdateRowsEvent,
    WriteRowsEvent,
)
from pymysqlreplication.event import (
    # QueryEvent,
    GtidEvent,
)
import decimal
# mysql binlog event
GTID_LOG_EVENT = 0x21

def get_data_from_mysql(sql=''):
    try:
        mysql_conn = pymysql.connect(**MYSQL_DB_INFO)
    except pymysql.Error as e:
        error = e.args
        return {"status_code": error[0],
                "msg": error[1]}

    cursor = mysql_conn.cursor(pymysql.cursors.DictCursor)

    try:
        cursor.execute('SET SESSION group_concat_max_len = 102400;')
        cursor.execute(sql)
    except pymysql.Error as e:
        error = e.args
        return {"status_code": error[0],
                "msg": error[1]}
    data = cursor.fetchall()

    cursor.close()
    mysql_conn.close()

    return {"status_code": 0,
            "data": data,
            }


def hget_from_redis(name='', key=''):
    """
    :param type: hget
    :param name: key name
    :return:
    """
    r = redis.StrictRedis(**REDIS_INFO)
    try:
        executed_gtid_set = r.hget(name=name, key=key)
        return executed_gtid_set
    except Exception as e:
        messages = 'exec redis command failed: %s' % str(e,)
        logger.error(messages)
        alarm(alarm_cnf, title=alarm_title, messages=messages)
        exit(1)
    finally:
        r.close()


def hset_to_redis(name='', key='', value=None):
    """
    :param name:
    :param key:
    :param value:
    :return:
    """
    r = redis.StrictRedis(**REDIS_INFO)
    try:
        r.hset(name, key, value)
        # print('set ok')
    except Exception as e:
        messages = 'exec redis command failed: %s' % str(e,)
        logger.error(messages)
        alarm(alarm_cnf, title=alarm_title, messages=messages)
        exit(1)
    finally:
        r.close()


def hmset_to_redis(name='', **mapping):
    """
    :param name:
    :param mapping:
    :return:
    """
    r = redis.StrictRedis(**REDIS_INFO)
    try:
        r.hmset(name, mapping)
        # print('set ok')
    except Exception as e:
        messages = 'exec redis command failed: %s' % str(e,)
        logger.error(messages)
        alarm(alarm_cnf, title=alarm_title, messages=messages)
        exit(1)
    finally:

        r.close()


class GetInfoMation(object):
    def __init__(self):
        self.alarm_title = alarm_title

    #获取需要同步表的主键或者唯一键，如果如果表不存在主键，则选择唯一键
    def get_primary_unique_key(self, dbname='', table_name=''):
        sql = f"""select concat(a.TABLE_SCHEMA,'.',a.TABLE_NAME) TABLE_NAME,a.COLUMN_NAME from 
            (select TABLE_SCHEMA,TABLE_NAME,COLUMN_NAME,(case COLUMN_KEY when 'PRI' then 1 else 2 end) key_type
             from information_schema.COLUMNS where TABLE_SCHEMA='{dbname}' and table_name= '{table_name}' and COLUMN_KEY in ('UNI','PRI')) a 
            join 
            (select TABLE_SCHEMA,TABLE_NAME,COLUMN_NAME,min((case COLUMN_KEY when 'PRI' then 1 else 2 end)) key_type
             from information_schema.COLUMNS where TABLE_SCHEMA='{dbname}' and table_name= '{table_name}' and COLUMN_KEY in ('UNI','PRI')
             group by TABLE_SCHEMA,TABLE_NAME) b 
            on a.TABLE_SCHEMA=b.TABLE_SCHEMA and a.TABLE_NAME=b.TABLE_NAME  and a.key_type=b.key_type;"""
        data = get_data_from_mysql(sql=sql)
        primary_unique_key = []
        if data['status_code'] == 0:
            for tbinfo in data['data']:
                primary_unique_key.append(tbinfo['COLUMN_NAME'])
            return primary_unique_key
        else:
            messages = 'get get_primary_unique_key failed : %s' % data['msg']
            logger.error(messages)
            alarm(alarm_cnf, title=self.alarm_title, messages=messages)
            exit(3)

    def get_mysql_postion(self):
        sql = """show master status"""
        data = get_data_from_mysql(sql=sql)
        if data['status_code'] == 0:
            file =data['data'][0]['File']
            position =data['data'][0]['Position']

            return {'file': file, 'position': position}
        else:
            messages = 'get master status failed : %s' % data['msg']
            logger.error(messages)
            alarm(alarm_cnf, title=self.alarm_title, messages=messages)
            exit(3)

    def get_executed_gtid_set_from_mysql(self):
        sql = """show master status"""
        data = get_data_from_mysql(sql=sql)
        if data['status_code'] == 0:
            executed_gtid_set =data['data'][0]['Executed_Gtid_Set']
            return executed_gtid_set
        else:
            messages = 'get get_primary_unique_key failed : %s' % data['msg']
            logger.error(messages)
            alarm(alarm_cnf, title=self.alarm_title, messages=messages)
            exit(3)

    def get_ch_column_type(self, db, table):
        """ :returns like :
        {'id': 'UInt32', 'report_id': 'Int32', 'cost_type': 'Int16', 'cost_name': 'String', 'amount': 'Int32', 'actual_amount': 'Int32'}
        """
        sql = "select name,type from system.columns where database='{0}' and table='{1}'".format(db, table)
        try:
            client = Client(**CLICKHOUSE_DB_INFO)
            ret = client.execute(sql)
            client.disconnect()
            return {d[0]: d[1] for d in ret}
        except Exception as error:
            messages = "获取clickhouse里面的字段类型错误. %s" % (str(error))
            logger.error(messages)
            alarm(alarm_cnf, title=self.alarm_title, messages=messages)
            exit(1)

    def get_db_list_to_ch(self):
        if cnf['only_schemas']:
            return cnf['only_schemas']['schemas']
        else:
            sql = """select group_concat(SCHEMA_NAME) SCHEMA_NAME 
            from information_schema.SCHEMATA 
            where SCHEMA_NAME not in ('sys','performance_schema','mysql','information_schema');"""
            data = get_data_from_mysql(sql=sql)

            if data['status_code'] == 0:
                schemas =data['data'][0]['SCHEMA_NAME']
                return list(set(schemas.split(','))-set(cnf['ignored_schemas']['schemas'].split(',')))
            else:
                messages = 'get db list failed: %s' % data['msg']
                logger.error(messages)
                alarm(alarm_cnf, title=self.alarm_title, messages=messages)

    def get_executed_gtid_set(self, name):
        executed_gtid_set = hget_from_redis(name=name, key='executed_gtid_set')
        if executed_gtid_set:
            return executed_gtid_set.decode()
        else:
            return cnf['mysql_server']['init_executed_gtid_set']

    def get_log_file(self, name):
        executed_gtid_set = hget_from_redis(name=name, key='log_file')
        if executed_gtid_set:
            return executed_gtid_set.decode()
        else:
            return cnf['mysql_server']['log_file']

    def get_log_pos(self, name):
        executed_gtid_set = hget_from_redis(name=name, key='log_pos')
        if executed_gtid_set:
            return executed_gtid_set.decode()
        else:
            return cnf['mysql_server']['log_pos']


class CheckInfoMationFirst(GetInfoMation):
    def __init__(self):
        super().__init__()
        self.only_schemas = None
        self.only_tables = None
        self.ignored_schemas = ['sys', 'mysql', 'information_schema', 'performance_schema']
        self.ignored_tables = None

        self.only_schemas_str = None
        self.only_tables_str = None
        self.ignored_schemas_str = None
        self.ignored_tables_str = None
        filters = cnf['filters']
        if cnf['filters'].get('only_schemas', None):
            self.only_schemas = filters['only_schemas'] if isinstance(filters['only_schemas'], list) else filters['only_schemas'].split(',')
            self.only_schemas_str = "','".join(self.only_schemas)
        if cnf['filters'].get('only_tables', None):
            self.only_tables = filters['only_tables'] if isinstance(filters['only_tables'], list) else filters['only_tables'].split(',')
            self.only_tables_str = "','".join(self.only_tables)
        if cnf['filters'].get('ignored_schemas', None):
            self.ignored_schemas.extend(filters['ignored_schemas'] if isinstance(filters['ignored_schemas'], list) else filters['ignored_schemas'].split(','))
            self.ignored_schemas_str = "','".join(self.ignored_schemas)
        if cnf['filters'].get('ignored_tables', None):
            self.ignored_tables = filters['ignored_tables'] if isinstance(filters['ignored_tables'], list) else filters['ignored_tables'].split(',')
            self.ignored_tables_str = "','".join(self.ignored_tables)

        self.rsnyc_tables =None

    def check_redis_connect(self):
        r = redis.StrictRedis(**REDIS_INFO)
        try:
            r.info()
            logger.info('redis status is ok')
            return True
        except Exception as e:
            logger.error('can not connect redis,exiting: %s' % str(e))
            return False
        finally:
            r.close()

    def check_is_gtid(self):
        sql = """show variables where Variable_name in ('gtid_mode','enforce_gtid_consistency');"""
        data = get_data_from_mysql(sql=sql)
        if data['status_code'] == 0:
            if data['data'][0]['Value'] == 'ON' and data['data'][1]['Value'] == 'ON':
                logger.info('mysql gtid mode and enforce_gtid_consistency is ON')
                return True
            else:
                return False
        else:
            logger.error('get mysql variables status failed : %s' % data['msg'])
            return False

    def check_clickhouse_status(self):
        sql = "show databases"
        try:
            client = Client(**CLICKHOUSE_DB_INFO)
            client.execute(sql)
            client.disconnect()
            messages = "clickhouse status is ok"
            logger.info(messages)
            return True
        except Exception as error:
            messages = "clickhouse can not connection：%s" % (str(error))
            logger.error(messages)
            return False

    def check_rsync_table_has_uniq_or_primary_key(self):
        if self.only_schemas_str:
            only_schemas_sql= "and table_schema in ('%s')" % self.only_schemas_str
        else:
            only_schemas_sql=''
        if self.only_tables_str:
            only_tables_sql= "and table_name in ('%s')" % self.only_tables_str
        else:
            only_tables_sql=''
        if self.ignored_schemas_str:
            ignored_schemas_sql= "and table_schema not in ('%s')" % self.ignored_schemas_str
        else:
            ignored_schemas_sql=''
        if self.ignored_tables_str:
            ignored_tables_sql= "and table_name not in ('%s')" % self.ignored_tables_str
        else:
            ignored_tables_sql=''

        sql = """select table_schema,table_name from information_schema.tables
         where TABLE_TYPE='BASE TABLE' and table_schema not in ('sys', 'mysql', 'information_schema', 'performance_schema')  
        %s %s %s %s;""" % (only_schemas_sql,
                           only_tables_sql,
                           ignored_schemas_sql,
                           ignored_tables_sql)
        data = get_data_from_mysql(sql=sql)
        if data['status_code'] == 0:
            self.rsnyc_tables = data['data']
            no_pri_or_uniq_index_tables = []
            if data['status_code'] == 0:
                for i in data['data']:
                    table_schema = i['table_schema']
                    table_name = i['table_name']
                    check_sql = """select count(*) countnum from information_schema.COLUMNS where COLUMN_KEY in ('UNI','PRI') and TABLE_SCHEMA='%s' and table_name ='%s' ;""" % (table_schema, table_name)
                    check_data = get_data_from_mysql(sql=check_sql)
                    if check_data['data'][0]['countnum'] == 0:
                        no_pri_or_uniq_index_tables.append(table_schema+'.'+table_name)
            else:
                logger.error('get mysql  tables info  failed : %s' % data['msg'])
            if no_pri_or_uniq_index_tables:
                logger.error('tables: %s has no uniq or primary key ' % ','.join(no_pri_or_uniq_index_tables))
                return False
            else:
                logger.info(' all tables:  has  uniq or primary key ')
                return True
        else:
            logger.error('get mysql tables status failed : %s' % data['msg'])
            return False

    def check_table_exists_on_mysql(self):
        """
        检查配置文件里面的表是否在mysql里面存在
        """
        tbs = []
        no_exists_table_in_mysql = None
        if self.only_tables_str:
            sql = """select table_schema,table_name from information_schema.tables where TABLE_TYPE='BASE TABLE' and table_schema in ('%s') and table_name in ('%s')""" % (self.only_schemas_str, self.only_tables_str)
            data = get_data_from_mysql(sql=sql)
            if data['status_code'] == 0:
                if data['data']:
                    for info in data['data']:
                        tbs.append(info['table_name'])
                    no_exists_table_in_mysql = list(set(self.only_tables) -set(tbs))
                else:
                    no_exists_table_in_mysql = self.only_tables
            else:
                logger.error('get mysql  tables info  failed : %s' % data['msg'])
        if no_exists_table_in_mysql:
            logger.error('tables: %s not in mysql' % ','.join(no_exists_table_in_mysql))
            return False
        else:
            logger.info('tables: %s  in mysql' % self.only_tables_str)
            return True

    def check_table_exists_on_clickhouse(self):
        """
        检查mysql需要同步的表在clickhouse里面是否存在
        :return:
        """
        client = Client(**CLICKHOUSE_DB_INFO)
        not_in_clickhouse_tables = []
        if not self.rsnyc_tables:
            messages = "no tables to sync "
            logger.error(messages)
            return False
        else:
            try:
                for i in self.rsnyc_tables:
                        table_schema = i['table_schema']
                        table_name = i['table_name']
                        check_sql = """select count(*) countnum from  system.tables where database= '%s' and name='%s';""" % (table_schema, table_name)
                        countnum = client.execute(check_sql)
                        if countnum[0][0] == 0:
                            not_in_clickhouse_tables.append(table_schema+'.'+table_name)
            except Exception as error:
                messages = "clickhouse failed: %s" % (str(error))
                logger.error(messages)
            finally:
                if client:
                    client.disconnect()

            if not_in_clickhouse_tables:
                messages = "tables: %s not in clickhouse" % (','.join(not_in_clickhouse_tables))
                logger.error(messages)
                return False
            else:
                messages = "tables all in clickhouse"
                logger.info(messages)
                return True

    def check_frist(self):
        ret = True
        logger.info('begin check:info at frist')
        logger.info('begin check: mysql gtid mode:gtid_mode and enforce_gtid_consistency mast is ON')
        if cnf['mysql_server']['gtid_mode'] == 1:
            if not self.check_is_gtid():
                ret = False
            logger.info('begin check redis status')
        if not self.check_redis_connect():
            ret = False
        logger.info('begin check clickhouse status')
        if not self.check_clickhouse_status():
            ret = False
        logger.info('begin check will sync tables is or not in mysql')
        if not self.check_table_exists_on_mysql():
            ret = False
        logger.info('begin check will sync tables has uniq or primary keys')
        if not self.check_rsync_table_has_uniq_or_primary_key():
            ret = False
        logger.info('begin check will sync tables is  or not in clickhouse')
        if not self.check_table_exists_on_clickhouse():
            ret = False
        if not ret:
            logger.error('exiting')
            exit(3)

    def check_table_column_on_mysql_not_on_clickhouse(self):
        pass


class GetDataFromMysqlBinlogToCh(CheckInfoMationFirst):
    def __init__(self):
        """
        """
        super().__init__()
        self.primary_keys = {}
        self.clickhouse_table_column_types = {}
        self.delete_data_list = []
        self.insert_data_list = []
        self.delete_primary_key_row_values = {}
        self.insert_primary_key_row_values = {}
        self.sequence = 0
        self.insert_events_count_all = 0
        self.update_events_count_all = 0
        self.delete_events_count_all = 0
        self.insert_events_count_last = 0
        self.update_events_count_last = 0
        self.delete_events_count_last = 0

        self.start_time = int(time.time())
        self.name = cnf['name']
        self.time_cost_last_insert = 0
        self.time_cost_last_delete = 0

        self.last_executed_gtid = {}
        self.last_gtid=''
        self.log_file=''
        self.log_pos=4

    def check_ch_is_or_not_failed_in_mutations(self):
        only_schemas_str = ','.join(self.only_schemas)
        # mutation_count__sql="""select count(*) from system.mutations where is_done=0 and database in (%s)""" % (only_schemas_str,)
        mutation_sql = """select * from system.mutations where is_done=0 and database in (%s) limit 10""" % (only_schemas_str,)
        client = Client(**CLICKHOUSE_DB_INFO)
        try:
            mutations_faild = client.execute(mutation_sql)
            if len(mutations_faild) > 0:
                pass
        except Exception as error:
            title = '%s' % self.name
            messages = "检查clickhouse mutations异常: %s" % (str(error))
            alarm(alarm_cnf=alarm_cnf, title=title, messages=messages)
            logger.error(messages)
            exit(1)

    def delete_data_in_ch(self):
        client = Client(**CLICKHOUSE_DB_INFO)
        delete_data_last = {}
        if not self.delete_data_list:
            self.time_cost_last_delete = 0
        else:
            logger.info("begin group data with primary_key_values to each table")
            for i in self.delete_data_list:
                delete_data_last.setdefault(i['table_name'], []).append(i['primary_key_values'])
            logger.info("group  each table data for one sql")
            for table_name, values in delete_data_last.items():
                sql_cur = {}
                for v in values:
                    for k, vv in v.items():
                        sql_cur.setdefault(k, []).append(vv)

                last_delete = ' and '.join(['%s in %s' % (k, tuple(v)) for k, v in sql_cur.items()])
                exec_sql = "alter table %s delete where %s ;" % (table_name, last_delete)
                begin_delete_time = int(time.time())
                logger.info("begin execute: exec_sql:%s" % exec_sql)
                client.execute(exec_sql)
                logger.info("end execute")
                client.disconnect()
                self.time_cost_last_delete = int(time.time())-begin_delete_time

    def specialty_logic_to_insert_data(self, column_type=None, data=None):
        """
        # 处理insert语句里面的，None值，一些特殊的默认值，decimal,dict转换为字符串，修改非标准时间等等默认值等
        # column_type = {'id': 'UInt32', 'report_id': 'Int32', 'cost_type': 'Int16', 'cost_name': 'String', 'amount': 'Int32', 'actual_amount': 'Int32'}
        # data = {'id': '', 'report_id': None, 'cost_type': '', 'cost_name': None, 'amount': '', 'actual_amount': ''}
        :return:
        """
        int_list = ['Int8', 'Int16', 'Int32', 'Int64', 'UInt8', 'UInt16', 'UInt32', 'UInt64']
        for key, value in data.items():
            if value == datetime.datetime(1970, 1, 1, 0, 0):
                data[key] = datetime.datetime(1970, 1, 2, 14, 1)

            # 处理mysql里面是Null的问题
            if value is None:
                if column_type[key] == 'DateTime':
                    data[key] = datetime.datetime(1970, 1, 2, 14, 1)
                elif column_type[key] == 'Date':
                    data[key] = datetime.date(1970, 1, 2)
                elif column_type[key] == 'String':
                    data[key] = ''
                elif column_type[key]in int_list:
                    data[key] = 0
                elif column_type[key] == 'Decimal':
                    data[key] = 0.0

            # decimal 字段类型处理，后期ch兼容mysql协议可以删除
            if type(value) == decimal.Decimal:
                if column_type[key] == 'String':
                    data[key] = str(value)

            if type(value) == dict:
                data[key] = str(value)
        return data

    def insert_data_to_ch(self):
        client = Client(**CLICKHOUSE_DB_INFO)
        insert_data_last = {}
        if not self.insert_data_list:
            self.time_cost_last_insert = 0
        else:
            # 处理insert语句里面的，None值，一些特殊的默认值，decimal,dict转换为字符串，修改非标准时间等等默认值等,并将同一个表的数据添加到一个列表里面
            for i in self.insert_data_list:
                logger.info("begin get clickhouse_table_column_types")
                if i['table_name'] not in self.clickhouse_table_column_types:
                    table_column_type_dict = get_info_ins.get_ch_column_type(i['schema'], i['table'])
                    self.clickhouse_table_column_types[i['table_name']] = table_column_type_dict
                else:
                    table_column_type_dict = self.clickhouse_table_column_types[i['table_name']]

                logger.info("begin clean data:some default values, decimal ,dict to string ")
                self.specialty_logic_to_insert_data(column_type=table_column_type_dict, data=i['values'])
                insert_data_last.setdefault(i['table_name'], []).append(i['values'])
                logger.info("after clean data: %s" % (insert_data_last,))
            # 将insert 数据生成insert 语句，并插入到clickhouse，在这之前将处理字段大小写问题，将mysql的字段按照clickhouse的字段生成
            for table_name, values in insert_data_last.items():
                table_columns_str = ','.join([j for key in values[0] for j in self.clickhouse_table_column_types[table_name] if key.upper() == j.upper()])
                exec_sql = "INSERT INTO %s (%s) VALUES" % (table_name, table_columns_str)
                # exec_sql="INSERT INTO %s  VALUES," % (table_name)
                logger.info("begin execute: exec_sql:%s" % exec_sql)
                logger.info("insert values:%s" % values)
                begin_insert_time = int(time.time())
                if values:
                    client.execute(exec_sql, params=values)
                logger.info("end execute!")
                self.time_cost_last_insert = int(time.time())-begin_insert_time

    def write_info_redis(self):
        """
        将同步信息和状态写入redis，方便后期做高可用和监控
        time_cost_last 一次操作的所有时间
        :return:
        """
        write_redis_info = {}
        self.insert_events_count_all = self.insert_events_count_all+self.insert_events_count_last
        self.update_events_count_all = self.update_events_count_all+self.update_events_count_last
        self.delete_events_count_all = self.delete_events_count_all+self.delete_events_count_last
        time_cost_last = int(time.time()) - self.start_time
        if cnf['mysql_server']['gtid_mode'] == 1:
            if self.last_executed_gtid:
                last_gtid_list = self.last_gtid.split(':')
                self.last_executed_gtid[last_gtid_list[0]] = '1-'+str(last_gtid_list[1])
            else:
                self.last_executed_gtid=gtid_str_to_dict(get_info_ins.get_executed_gtid_set(name=self.name))
            write_redis_info['executed_gtid_set'] = gtid_dict_to_str(self.last_executed_gtid)
        write_redis_info['insert_events_count_all'] = self.insert_events_count_all
        write_redis_info['update_events_count_all'] = self.update_events_count_all
        write_redis_info['delete_events_count_all'] = self.delete_events_count_all
        write_redis_info['insert_events_count_last'] = self.insert_events_count_last
        write_redis_info['update_events_count_last'] = self.update_events_count_last
        write_redis_info['delete_events_count_last'] = self.delete_events_count_last
        write_redis_info['time_cost_last'] = time_cost_last
        write_redis_info['time_cost_last_delete'] = self.time_cost_last_delete
        write_redis_info['time_cost_last_insert'] = self.time_cost_last_insert
        write_redis_info['time_commit_last'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        write_redis_info['log_file'] = self.log_file
        write_redis_info['log_pos'] = self.log_pos

        # print(write_redis_info)
        logger.info('write_redis_info: %s ' % str(write_redis_info))
        hmset_to_redis(self.name, **write_redis_info)

        # r = redis.StrictRedis(**REDIS_INFO)
        # print(r.hgetall(self.name))
        #初始化变量
        self.insert_events_count_last = 0
        self.update_events_count_last = 0
        self.delete_events_count_last = 0
        self.delete_data_list = []
        self.insert_data_list = []
        self.delete_primary_key_row_values = {}
        self.insert_primary_key_row_values = {}
        self.sequence = 0
        self.start_time = int(time.time())

    def binlog_from_event_handler(self, binlogevent):
        for row in binlogevent.rows:
            self.sequence += 1
            cur_insert_data = {"schema": binlogevent.schema, "table": binlogevent.table, "log_pos": binlogevent.packet.log_pos}
            event = {"schema": binlogevent.schema, "table": binlogevent.table}
            cur_delete_data = {}
            table_name = binlogevent.schema+'.'+binlogevent.table
            # logger.info(str(cur_insert_data))
            #查到当前表的主键或者唯一键，如果在primary_keys不存在，则去数据库查找，并存入primary_keys
            logger.info('table: %s begin get_primary_unique_key' % (table_name,))
            if table_name in self.primary_keys:
                primary_key = self.primary_keys[table_name]
            else:
                primary_key = get_info_ins.get_primary_unique_key(dbname=binlogevent.schema,
                                                                  table_name=binlogevent.table)
                if not primary_key:
                    messges = '%s has no uniq or primary key' % (table_name,)
                    logger.error(messges)
                    alarm(alarm_cnf, title=self.alarm_title, messages=messges)
                    exit(1)
                else:
                    self.primary_keys[table_name] = primary_key
            logger.info('%s: begin do DML event' % (table_name,))
            if isinstance(binlogevent, DeleteRowsEvent):
                self.delete_events_count_last += 1
                event["action"] = "delete"
                event["values"] = dict(row["values"].items())
                event = dict(event.items())
                # print('DeleteRowsEvent',event)
                messges='%s :do DeleteRowsEvent: begin get primary key or uniq key values' % (table_name,)
                logger.info(messges)
                primary_key_values = dict([(pri, event['values'][pri]) for pri in primary_key])
                primary_key_row_values = '_'.join([str(event['values'][pri]) for pri in primary_key])
                db_table_primary_key_row_values = table_name+'_'+primary_key_row_values
                messges='%s:check %s  is in self.insert_primary_key_row_values,if in :then delete row from self.insert_primary_key_row_values' % (table_name, db_table_primary_key_row_values)
                logger.info(messges)

                if db_table_primary_key_row_values in self.insert_primary_key_row_values:
                    messges='%s: %s is in self.insert_primary_key_row_values, delete row from self.insert_primary_key_row_values' % (table_name, db_table_primary_key_row_values)
                    logger.info(messges)
                    for index, key in enumerate(self.insert_data_list):
                        if key['db_table_primary_key_row_values'] == db_table_primary_key_row_values:
                            del self.insert_data_list[index]
                            del self.insert_primary_key_row_values[db_table_primary_key_row_values]

                cur_delete_data['table_name'] = table_name
                cur_delete_data['primary_key_values'] = primary_key_values
                cur_delete_data['sequence'] = self.sequence
                cur_delete_data['db_table_primary_key_row_values'] = db_table_primary_key_row_values
                cur_delete_data['primary_key'] = primary_key
                messges='%s: %s cur_delete_data: %s ' % (table_name, primary_key_row_values, str(cur_delete_data))
                logger.info(messges)
                self.delete_data_list.append(cur_delete_data)
                messges='%s: %s add: %s to self.delete_primary_key_row_values' % (table_name, primary_key_row_values, db_table_primary_key_row_values)
                logger.info(messges)
                self.delete_primary_key_row_values[db_table_primary_key_row_values] = self.sequence
            elif isinstance(binlogevent, UpdateRowsEvent):
                messges='%s :do UpdateRowsEvent' % (table_name,)
                logger.info(messges)
                self.update_events_count_last += 1
                # event["action"] = "update"
                event["before_values"] = dict(row["before_values"].items())
                event["after_values"] = dict(row["after_values"].items())
                event = dict(event.items())
                # print('UpdateRowsEvent',event)
                primary_key_row_values_before = '_'.join([str(event['before_values'][pri]) for pri in primary_key])
                primary_key_row_values_after = '_'.join([str(event['after_values'][pri]) for pri in primary_key])
                db_table_primary_key_row_values_before = table_name+'_'+primary_key_row_values_before
                db_table_primary_key_row_values_after= table_name+'_'+primary_key_row_values_after
                primary_key_values_before = dict([(pri, event['before_values'][pri]) for pri in primary_key])
                # primary_key_values_after = dict([(pri, event['after_values'][pri]) for pri in primary_key])
                messges='%s:%s :begin change update to delete and insert' % (table_name, primary_key_row_values_before)
                logger.info(messges)
                messges='check %s  is in self.insert_primary_key_row_values,if in self.insert_primary_key_row_values:then delete row from self.insert_primary_key_row_values' % (db_table_primary_key_row_values_before)
                logger.info(messges)
                if db_table_primary_key_row_values_before in self.insert_primary_key_row_values:
                    messges=' %s is in self.insert_primary_key_row_values, delete row from self.insert_primary_key_row_values and in self.insert_primary_key_row_values' % (db_table_primary_key_row_values_before)
                    logger.info(messges)
                    for index, key in enumerate(self.insert_data_list):
                        if key['db_table_primary_key_row_values'] == db_table_primary_key_row_values_before:
                            logger.info('delete %s from self.insert_data_list' % db_table_primary_key_row_values_before)
                            del self.insert_data_list[index]
                            logger.info('delete %s from self.insert_data_list ok ' % db_table_primary_key_row_values_before)
                            del self.insert_primary_key_row_values[db_table_primary_key_row_values_before]

                messges='check %s  is in self.delete_primary_key_row_values,if in :then delete row from self.delete_primary_key_row_values' % (db_table_primary_key_row_values_before)
                logger.info(messges)
                if db_table_primary_key_row_values_before in self.delete_primary_key_row_values:
                    messges=' %s is in self.delete_primary_key_row_values, delete row from self.delete_primary_key_row_values and in self.delete_data_list' % (db_table_primary_key_row_values_before)
                    logger.info(messges)
                    for index, key in enumerate(self.delete_data_list):
                        if key['db_table_primary_key_row_values'] == db_table_primary_key_row_values_before:
                            logger.info('delete %s from self.delete_data_list' % db_table_primary_key_row_values_after)
                            del self.delete_data_list[index]
                            del self.delete_primary_key_row_values[db_table_primary_key_row_values_before]

                messges='check %s  is in self.delete_primary_key_row_values,if in :then delete row from self.delete_primary_key_row_values' % (db_table_primary_key_row_values_after)
                logger.info(messges)
                if db_table_primary_key_row_values_after in self.delete_primary_key_row_values:
                    messges=' %s is in self.delete_primary_key_row_values, delete row from self.delete_primary_key_row_values and in self.delete_data_list' % (db_table_primary_key_row_values_after)
                    logger.info(messges)
                    for index, key in enumerate(self.delete_data_list):
                        if key['db_table_primary_key_row_values'] == db_table_primary_key_row_values_after:
                            logger.info('delete %s from self.delete_data_list' % db_table_primary_key_row_values_after)
                            del self.delete_data_list[index]
                            del self.delete_primary_key_row_values[db_table_primary_key_row_values_after]

                cur_delete_data['table_name'] = table_name
                cur_delete_data['primary_key_values'] = primary_key_values_before
                cur_delete_data['sequence'] = self.sequence
                cur_delete_data['db_table_primary_key_row_values'] = db_table_primary_key_row_values_before
                cur_delete_data['primary_key'] = primary_key
                self.delete_data_list.append(cur_delete_data)
                self.delete_primary_key_row_values[db_table_primary_key_row_values_before] = self.sequence
                messges='%s: %s cur_delete_data: %s ' % (table_name, db_table_primary_key_row_values_before, str(cur_delete_data))
                logger.info(messges)

                cur_insert_data['table_name'] = table_name
                cur_insert_data['values'] = event['after_values']
                cur_insert_data['sequence'] = self.sequence
                cur_insert_data['db_table_primary_key_row_values'] = db_table_primary_key_row_values_after
                cur_insert_data['primary_key'] = primary_key
                self.insert_data_list.append(cur_insert_data)
                self.insert_primary_key_row_values[db_table_primary_key_row_values_after] = self.sequence
                messges='%s: %s cur_insert_data: %s ' % (table_name, db_table_primary_key_row_values_after, str(cur_insert_data))
                logger.info(messges)
                messges='%s: %s self.insert_primary_key_row_values: %s ' % (table_name, db_table_primary_key_row_values_after, str(self.insert_primary_key_row_values))
                logger.info(messges)

            elif isinstance(binlogevent, WriteRowsEvent):
                messges='%s :do WriteRowsEvent' % (table_name,)
                logger.info(messges)
                self.insert_events_count_last += 1
                event["action"] = "insert"
                event["values"] = dict(row["values"].items())
                primary_key_row_values = '_'.join([str(event['values'][pri]) for pri in primary_key])
                db_table_primary_key_row_values = table_name+'_'+primary_key_row_values
                event = dict(event.items())
                # print('WriteRowsEvent',event)
                messges='%s:check %s  is in self.db_table_primary_key_row_values,if in :then delete row from self.insert_primary_key_row_values' % (table_name, db_table_primary_key_row_values)
                logger.info(messges)
                if db_table_primary_key_row_values in self.insert_primary_key_row_values:
                    messges='%s: %s is in self.insert_primary_key_row_values, delete row from self.insert_primary_key_row_values and in self.insert_data_list'  % (table_name, db_table_primary_key_row_values)
                    logger.info(messges)
                    for index, key in enumerate(self.insert_data_list):
                        if key['db_table_primary_key_row_values'] == db_table_primary_key_row_values:
                            del self.insert_data_list[index]
                            del self.insert_primary_key_row_values[db_table_primary_key_row_values]
                cur_insert_data['table_name'] = table_name
                cur_insert_data['values'] = event['values']
                cur_insert_data['db_table_primary_key_row_values'] = db_table_primary_key_row_values
                cur_insert_data['primary_key'] = primary_key
                self.insert_data_list.append(cur_insert_data)
                self.insert_primary_key_row_values[db_table_primary_key_row_values] = self.sequence
                messges='%s: %s cur_insert_data: %s ' % (table_name, primary_key_row_values, str(cur_insert_data))
                logger.info(messges)

            logger.info('%s: self.sequence: %s time: %s' % (table_name, self.sequence, int(time.time()) - self.start_time ))
            # print(int(cnf['bulk_insert_control']['rows_to_target']), int(cnf['bulk_insert_control']['interval']), int(time.time()) - self.start_time)


    def get_data_from_binlog_to_ch(self,):
        logger.info("""'only_schemas': %s,'only_tables': %s,'ignored_schemas': %s,'ignored_tables': %s """ % (
            self.only_schemas,
            self.only_tables,
            self.ignored_schemas,
            self.ignored_tables)
                    )
        if cnf['mysql_server']['gtid_mode'] == 1:
            stream = BinLogStreamReader(connection_settings=MYSQL_DB_INFO,
                                        server_id=cnf['mysql_server']['server_id'],
                                        blocking=True,
                                        only_events=[DeleteRowsEvent, WriteRowsEvent, UpdateRowsEvent, GtidEvent],
                                        auto_position=get_info_ins.get_executed_gtid_set(name=self.name),
                                        only_schemas=self.only_schemas,
                                        only_tables=self.only_tables,
                                        ignored_schemas=self.ignored_schemas,
                                        ignored_tables=self.ignored_tables,
                                        slave_heartbeat=10,
                                        fail_on_table_metadata_unavailable=True,
                                        freeze_schema=True,
                                        resume_stream=True)
        else:
            stream = BinLogStreamReader(connection_settings=MYSQL_DB_INFO,
                                        server_id=cnf['mysql_server']['server_id'],
                                        blocking=True,
                                        only_events=[DeleteRowsEvent, WriteRowsEvent, UpdateRowsEvent],
                                        log_file=get_info_ins.get_log_file(name=self.name),
                                        log_pos=int(get_info_ins.get_log_pos(name=self.name)),
                                        only_schemas=self.only_schemas,
                                        only_tables=self.only_tables,
                                        ignored_schemas=self.ignored_schemas,
                                        ignored_tables=self.ignored_tables,
                                        slave_heartbeat=10,
                                        fail_on_table_metadata_unavailable=True,
                                        freeze_schema=True,
                                        resume_stream=True)

        for binlogevent in stream:
            if binlogevent.event_type == GTID_LOG_EVENT:
                self.last_gtid=binlogevent.gtid
            else:
                self.log_file, self.log_pos = stream.log_file, stream.log_pos
                try:
                    self.binlog_from_event_handler(binlogevent)
                except Exception as e:
                    messages='%s : unknown error: %s' % ('binlog_from_event_handler',str(e))
                    logger.error(messages)
                    alarm(alarm_cnf=alarm_cnf, title=self.alarm_title, messages=messages)
                    exit(3)

            if self.sequence > int(cnf['bulk_insert_control']['rows_to_target']) or int(time.time()) - self.start_time > int(cnf['bulk_insert_control']['interval']):
                messges="""self.sequence> %s or time > %s,do  delete data,insert data,write redis""" % (int(cnf['bulk_insert_control']['rows_to_target']),int(cnf['bulk_insert_control']['interval']))
                logger.info(messges)
                messges='begin delete data from clickhouse'
                logger.info(messges)
                try:
                    self.delete_data_in_ch()
                except Exception as e:
                    messages = "delete data from clickhouse failed: %s" % str(e,)
                    logger.error(messages)
                    alarm(alarm_cnf=alarm_cnf, title=self.alarm_title, messages=messages)
                    exit(3)

                messges='begin insert data to clickhouse'
                logger.info(messges)
                try:
                    self.insert_data_to_ch()
                except Exception as e:
                    messages = "delete data from clickhouse failed: %s" % str(e,)
                    logger.error(messages)
                    alarm(alarm_cnf=alarm_cnf, title=self.alarm_title, messages=messages)
                    exit(3)

                # 生成程序当前状态，并写入redis，方便后期监控，和高可用
                messges = 'begin write infomation data to redis'
                logger.info(messges)
                try:
                    self.write_info_redis()
                except Exception as e:
                    messages = "write data to redis failed: %s" % str(e,)
                    logger.error(messages)
                    alarm(alarm_cnf=alarm_cnf, title=self.alarm_title, messages=messages)
                    exit(3)
def main():
    parser = init_parse()
    args = parser.parse_args()
    configfile = args.conf
    # logtoredis = args.logtoredis
    global cnf
    cnf = get_config(configfile)
    # print(cnf)
    global BINLOG_MYSQL_SETTINGS
    BINLOG_MYSQL_SETTINGS = {
        'host': cnf['mysql_server']['host'],
        'port': int(cnf['mysql_server']['port']),
        'user': cnf['mysql_server']['user'],
        'password': cnf['mysql_server']['passwd'],
        'db': '',
        'charset': 'utf8mb4',
        'connect_timeout': 10,
        'autocommit': True
    }
    global CLICKHOUSE_DB_INFO
    CLICKHOUSE_DB_INFO = {
        'host': cnf['clickhouse_server']['host'],
        'port': cnf['clickhouse_server']['port'],
        'user': cnf['clickhouse_server']['user'],
        'password': cnf['clickhouse_server']['passwd']
    }
    global REDIS_INFO
    REDIS_INFO = {
        'host': cnf['redis_server']['host'],
        'port': cnf['redis_server']['port'],
        'db': cnf['redis_server']['db'],
        'password': cnf['redis_server']['passwd']
    }
    global MYSQL_DB_INFO
    MYSQL_DB_INFO = copy.copy(BINLOG_MYSQL_SETTINGS)
    MYSQL_DB_INFO['read_timeout'] = 20
    logger_cnf = cnf['log']
    global logger
    logger = log_handler(logger_cnf=logger_cnf)
    global alarm_cnf
    alarm_cnf=cnf['failure_alarm']
    global alarm_title
    alarm_title = 'sync data from mysql to clickhouse, task: %s' % cnf['name']
    global check_at_frist
    check_at_frist=CheckInfoMationFirst()
    check_at_frist.check_frist()
    global get_info_ins
    get_info_ins = GetInfoMation()
    main=GetDataFromMysqlBinlogToCh()
    try:
        main.get_data_from_binlog_to_ch()
    except Exception as e:
        messages = "unkown error: %s" % str(e,)
        logger.error(messages)
        alarm(alarm_cnf=alarm_cnf, title=alarm_title, messages=messages)
        exit(3)


if __name__ == '__main__':
    main()
