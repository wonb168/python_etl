# 用于打包成可运行文件
import six
import packaging
import packaging.version
import packaging.specifiers
import packaging.requirements
# etl所需包
import pandas as pd 
import configparser 
import os,sys
# 连接数据库
import pymssql #sqlserver
import cx_Oracle as oracle #oracle
import psycopg2 #pg驱动
from sqlalchemy import create_engine #orm框架


# 读配置文件
cf = configparser.ConfigParser()
# path=os.path.abspath(sys.argv[0])
# print(path)
# path2=os.path.realpath(sys.argv[0]) 
# print(path2)
# 获取调用的py文件所在路径
dirname, filename = os.path.split(os.path.abspath(sys.argv[0])) 
# dirname = os.path.dirname(os.path.abspath('.'))  # 获取当前文件所在目录的上一级目录，即项目所在目录E:\Crawler
print(dirname)
cf.read(dirname+"/config.ini")# 读取配置文件，如果写文件的绝对路径，就可以不用os模块
# secs = cf.sections()  # 获取文件中所有的section(一个配置文件中可以有多个配置，如数据库相关的配置，邮箱相关的配置，每个section由[]包裹，即[section])，并以列表的形式返回
# print(secs)

# options = cf.options(db)  # 获取某个section名为Mysql-Database所对应的键
# print(options)

# items = cf.items(db)  # 获取section名为Mysql-Database所对应的全部键值对
# print(items)

db = cf.get("config", "from")
sql = cf.get("config", "sql")
table = cf.get("config", "table")
# 源
host = cf.get(db, "host")  # 获取[]中host对应的值
port = cf.get(db, "port")
user = cf.get(db, "user")
password = cf.get(db, "password")
dbname = cf.get(db, "dbname")
# 目标


print(db)
if db=='postgre':
    # 读pg
    # conn = psycopg2.connect(dbname="quanfuyuan", user="quanfuyuan",password="Onms8tMs", host="192.168.16.183", port="5432")
    conn = psycopg2.connect(dbname=dbname, user=user,password=password, host=host, port=port)
elif db=='oracle':
    # 读oracle
    # conn = oracle.connect('system/123456@192.168.16.235:1521/orcl')
    conn = oracle.connect(user+'/'+password+'@'+host+':'+port+'/'+dbname)
elif db=='sqlserver':
    # 读sqlserver
    # conn = pymssql.connect(host='192.168.16.183',port='1433',user='sa',password='Lzsj2019',database='master',charset="utf8")
    conn = pymssql.connect(host=host,port=port,user=user,password=password,database=dbname,charset="utf8")
else:
    print('暂时只支持oracle、sqlserver、postgre数据库，其他请与作者联系，微信37640893！')
# 查询sql
print(conn)
sql_query = sql
print(sql_query)
df = pd.read_sql_query(sql_query, con=conn)

# 写csv
csv=dirname+'/'+table+'.csv'
df.to_csv(csv, index=False, header=True)
print('数据已导出到：'+table)
# 写db
dbschema='ods'
engine = create_engine('postgresql://quanfuyuan:Onms8tMs@192.168.16.183:5432/quanfuyuan',connect_args={'options': '-csearch_path={}'.format(dbschema)})
df.to_sql(table,engine,index=False,if_exists='replace')