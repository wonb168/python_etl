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
from sqlalchemy.orm import sessionmaker



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
cf.read(dirname+"/config.ini")# 读取当前目录下的配置文件，如果写文件的绝对路径，就可以不用os模块
# secs = cf.sections()  # 获取文件中所有的section(一个配置文件中可以有多个配置，如数据库相关的配置，邮箱相关的配置，每个section由[]包裹，即[section])，并以列表的形式返回
# print(secs)

# options = cf.options(db)  # 获取某个section名为Mysql-Database所对应的键
# print(options)

# items = cf.items(db)  # 获取section名为Mysql-Database所对应的全部键值对
# print(items)


# 源src, 输入sql
src = cf.get("config", "from")
sql = cf.get("config", "sql")
host = cf.get("from", "host")  # 获取[]中host对应的值
port = cf.get("from", "port")
user = cf.get("from", "user")
password = cf.get("from", "password")
dbname = cf.get("from", "dbname")
print("数据源："+src)

# 目标obj
obj = cf.get("config", "to")
table = cf.get("config", "table")
host2 = cf.get('to', "host")  # 获取[]中host对应的值
port2 = cf.get('to', "port")
user2 = cf.get('to', "user")
password2 = cf.get('to', "password")
dbname2 = cf.get('to', "dbname")
print("目的地："+obj)

if src=='postgre':
    # 读pg
    # conn = psycopg2.connect(dbname="quanfuyuan", user="quanfuyuan",password="Onms8tMs", host="192.168.16.183", port="5432")
    conn = psycopg2.connect(dbname=dbname, user=user,password=password, host=host, port=port)
elif src=='oracle':
    conn = oracle.connect(user+'/'+password+'@'+host+':'+port+'/'+dbname)
elif src=='sqlserver':
    # 读sqlserver
    # conn = pymssql.connect(host='192.168.16.183',port='1433',user='sa',password='Lzsj2019',database='master',charset="utf8")
    conn = pymssql.connect(host=host,port=port,user=user,password=password,database=dbname,charset="utf8")
else:
    print('暂时只支持oracle、sqlserver、postgre数据库和csv，其他请与作者联系，微信37640893！')
# 查询sql
print(conn)
print("开始读取"+src)
sql_query = sql
print(sql_query)
df = pd.read_sql_query(sql_query, con=conn)


print("开始写入"+obj)
# 写csv
if obj=='csv':
    csv=dirname+'/'+table+'.csv'
    df.to_csv(csv, index=False, header=True)
    print('数据已导出到：'+table)
# 写db
elif (obj=='postgre'  or obj=='pg'):
    dbschema='ods'
    dbtype='postgresql'
#     # engine = create_engine('postgresql://'+user+':'+password+'@'+host+':'+port+'/'+dbname,connect_args={'options': '-csearch_path={}'.format(dbschema)})
#     df.to_sql(table,engine,index=False,if_exists='replace')
elif (obj=='oracle' ):
    dbtype='oracle'
#     export_env='export ORACLE_HOME='+dirname+'/instantclient_11_2'
#     print(export_env)
#     os.system(export_env)
#     dbschema='ods'
#     engine=create_engine('oracle://'+user+':'+password+'@'+host+':'+port+'/'+dbname, echo=True)
#     df.to_sql(table,engine,index=False,if_exists='replace')
elif (obj=='sqlserver'  or obj=='mssql'):
    dbtype='mssql+pymssql'
    dbschema='ods'

#     engine = create_engine('mssql+pymssql://'+user+':'+password+'@'+host+':'+port+'/''+name',echo=True)
#     df.to_sql(table,engine,index=False,if_exists='replace')
engine = create_engine(dbtype+'://'+user2+':'+password2+'@'+host2+':'+port2+'/'+dbname2,connect_args={'options': '-csearch_path={}'.format(dbschema)})
print(engine)
print(dbschema)
# if dbschema.strip():
#     DB_Session = sessionmaker(bind=engine)
#     session = DB_Session()
#     sql='create schema if not exists '+dbschema+';'
#     print(sql)
#     session.execute(sql)
df.to_sql(table,engine,index=False,if_exists='replace')
print('写入表名：'+table)