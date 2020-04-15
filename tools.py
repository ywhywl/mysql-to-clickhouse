#!/usr/bin/env python3
# encoding: utf-8
"""
@by: wenlongy
@contact: 247540299@qq.com
@PROJECT_NAME: mysql-to-clickhouse
@file: tools.py
@time: 2020-04-11 22:03
"""
#  ##email
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.header import Header
import argparse
import os
import toml
import logging
import socket


def init_parse():
    parser = argparse.ArgumentParser(
        prog='Data Replication to clikhouse',
        description='mysql data is copied to clikhouse',
    )
    parser.add_argument('-c', '--conf', required=False, default='./metainfo.toml', help='config file')
    parser.add_argument('-d', '--debug', action='store_true', default=False, help='Display SQL info')
    return parser


def get_host_ip():
    """
    查询本机ip地址
    :return: ip
    """
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(('8.8.8.8', 80))
        ip = s.getsockname()[0]
    finally:
        s.close()

    return ip


# 获取配置文件参数的函数
def get_config(configfile):

    if not os.path.exists(configfile):
        print("指定的配置文件: %s 不存在" % (configfile))
        exit(1)

    with open(configfile, "r", encoding="utf-8") as fs:
        t_data = toml.load(fs)
    return t_data


def gtid_str_to_dict(gtid_purged_str):
    return {k.split(':')[0]: k.split(':')[1] for k in gtid_purged_str.split(',')}


def gtid_dict_to_str(gtid_dict):
    return ','.join([k+':'+v for k, v in gtid_dict.items()])


# 邮件发送函数
def send_mail(send_mail_conf=None, subject='', messages='', **atts):
    # sender 发件人地址
    # atts #附件列表
    # receivers # 接受者列表 ['yaowenlong@yunniao.cn']
    # sender = 'finance_report_new@yunniao.cn'
    # receivers = ['yaowenlong@yunniao.cn']  # 接收邮件，可设置为你的QQ邮箱或者其他邮箱
    # subject  #  'test'  邮件标题
    # messages  # 邮件内容

    # 第三方 SMTP 服务
    # mail_host = mail_host  #设置服务器
    # mail_user = mail_user   #用户名
    # mail_pass = mail_pass   #口令
    #创建一个带附件的实例
    message = MIMEMultipart()
    # message['From'] = Header("财务报表", 'utf-8')
    # message['To'] = Header("%s" % receivers, 'utf-8')
    # message['To'] = "%s" % receivers
    message['Subject'] = Header(subject, 'utf-8')

    #邮件正文内容
    # message.attach(MIMEText('%s' % messages, 'plain', 'utf-8'))
    message.attach(MIMEText('%s \n\n\n\n\n send by %s' % (messages, get_host_ip()), 'plain', 'utf-8'))

    # 构造附件，传送当前目录下的 test.txt 文件
    if len(atts) >= 1:
        for filename, filepath in atts.items():
            att1 = MIMEText(open(filepath, mode='rb').read(), 'base64', 'utf-8')
            att1["Content-Type"] = 'application/octet-stream'
            # 这里的filename可以任意写，写什么名字，邮件中显示什么名字
            att1["Content-Disposition"] = 'attachment; filename="%s"' % filename
            message.attach(att1)
    try:
        smtpObj = smtplib.SMTP()
        # smtpObj.set_debuglevel(1)
        smtpObj.connect(send_mail_conf['mail_host'], send_mail_conf['mail_port'])    # 25 为 SMTP 端口号
        smtpObj.login(send_mail_conf['mail_user'], send_mail_conf['mail_pass'])
        xx = smtpObj.sendmail(send_mail_conf['sender'], send_mail_conf['receivers'].split() if isinstance(send_mail_conf['receivers'],str) else send_mail_conf['receivers'], message.as_string())
        print("邮件发送成功: %s" % xx)
    except Exception as e:
        print(e)
        print("Error: 无法发送邮件: %s" % send_mail_conf['mail_user'])


def alarm(alarm_cnf=None, title='', messages=''):

    send_mail(send_mail_conf=alarm_cnf, subject=title, messages=messages)


def log_handler(logger_cnf):
    logfile = logger_cnf['log_path']
    log_level = logger_cnf['log_level']
    console_level = logger_cnf['console_level']
    logger_handler = logging.getLogger(logfile)
    if not logger_handler.handlers:
        eval("""logger_handler.setLevel(logging.%s)""" % log_level.upper())
        logfmt = "%(asctime)s %(filename)s [%(funcName)s] [line:%(lineno)d] %(levelname)s: %(message)s"
        datefrm = "%Y-%m-%d %H:%M:%S"
        formatter = logging.Formatter(logfmt, datefrm)
        fh = logging.FileHandler(logfile, encoding='UTF-8', mode='a')
        fh.setFormatter(formatter)
        logger_handler.addHandler(fh)
        #定义一个StreamHandler，将INFO级别或更高的日志信息打印到标准错误，并将其添加标准输出#
        console = logging.StreamHandler()
        # console.setLevel(logging.WARN)
        eval("""console.setLevel(logging.%s)""" % console_level.upper())

        console.setFormatter(formatter)
        logger_handler.addHandler(console)
    return logger_handler


def main():
    configfile = './metainfo.toml'
    cnf = get_config(configfile)
    ###mail test
    title = '这是一次邮件测试'
    messages = '这是一次邮件测试 dafdsfasfasdfasfasdf '
    # #添加附件发送
    # atts = {
    #     # '保险.xlsx': '/tmp/保险_11.xlsx'
    #     'ccccc.xlsx': '/Users/wenlongy/Downloads/powerdns.sql'
    #     # 'powerdns1.sql': '/Users/wenlongy/Downloads/powerdns.sql'
    # }
    # # send_mail(send_mail_conf=cnf['failure_alarm'], subject=title, messages=messages, **atts)
    send_mail(send_mail_conf=cnf['failure_alarm'], subject=title, messages=messages)
    logfile = cnf['log']['log_path']
    # logger test
    # log_level = cnf['log']['log_level']
    # logger = log_handler(logfile, log_level)
    # logger.info('Master_Port: 11af')
    # logger.warning('Master_Port: 11af')
    #
    # logfile = 'app.log'
    # log_level = 'warning'
    # logger1 = log_handler(logfile, log_level)
    # logger1.info('Master_Port: 11af')
    # logger1.warning('Master_Port: 11af')


if __name__ == '__main__':
    main()
