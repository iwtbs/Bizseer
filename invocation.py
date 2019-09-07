# -*- coding: utf-8 -*-

"""
@Date: 2019/9/1

@Author: wzk

@Summary: es_data and db_data to invocation
"""
from elasticsearch import Elasticsearch
from itertools import groupby
from matplotlib import pyplot as plt
from tqdm import tqdm_notebook as tqdm
from influxdb import DataFrameClient
import click
import datetime
import os
import time
import pickle
import pandas as pd
import numpy as np
import pandas as pd
np.set_printoptions(suppress=True)
pd.set_option('display.float_format', lambda x: '%.0f' % x) #避免科学技术法


#品农给我的是北京时间，需要转换为timestamp进行数据提取
def string2timestamp(strValue)->float:
    """
    :param strValue: '2015-08-28 16:43:37'
    :return: 1440751417.0
    """        
    d = datetime.datetime.strptime(strValue, "%Y-%m-%d %H:%M")
    t = d.timetuple()
    timeStamp = int(time.mktime(t))
    timeStamp = float(str(timeStamp) + str("%06d" % d.microsecond))/1000000
    return timeStamp


def get_time(error_start_str):
    '''
    : get time
    : es/db start time, range is 15 min(timestamp +-600)   
    : param error_start_str: error start time, range is 3  min(timestamp +180)   eg: '2019-08-29 22:19'
    '''
    es_time_trans        =  10**6. #es的timestamp_millis查找比转换结果多3位
    influxdb_time_trans  = 10**9. #influxdb的timestamp查找比转换结果多9位
    error_time           = string2timestamp(error_start_str)
    error_start          = error_time  * es_time_trans
    error_end            = (error_time + 120) * es_time_trans
    time_start           = error_time - 600
    time_end             = error_time + 600
    #time_start           = string2timestamp('2019-08-29 22:15:00')
    #time_end             = string2timestamp('2019-08-29 22:30:00')
    es_start             = time_start * es_time_trans
    es_end               = time_end * es_time_trans
    influxdb_start       = int(time_start * influxdb_time_trans)  #int避免科学技术
    influxdb_end         = int(time_end * influxdb_time_trans)
    #error_start          = string2timestamp('2019-08-29 22:20:00.000000') * es_time_trans
    #error_end            = string2timestamp('2019-08-29 22:23:00.000000') * es_time_trans
    return es_start, es_end, influxdb_start, influxdb_end, error_start, error_end

#下面代码要获取指定时间es的数据，es_start是开始，es_end是结束，这里我取的开始结束间隔半个小时
def search_all(index, body)->list:
    """
    : param index: "zipkin:span-2019-08-29"
    : param body: sql_request
    """
    es = Elasticsearch(host = '192.168.115.84', port = 9200)
    rsp = es.search(index=index, body=dict(**body, size=1000), scroll='1m',request_timeout=30)
    total = rsp['hits']['total']
    print(total)
    scroll_id = rsp['_scroll_id']
    scroll_size = total
    with tqdm(total=total) as pbar:
        rets = []
        while scroll_size > 0:
            _rsp = es.scroll(scroll_id=scroll_id, scroll='1m')
            scroll_id = _rsp['_scroll_id']
            scroll_size = len(_rsp['hits']['hits'])
            total -= scroll_size
            rets.extend(parse(_rsp['hits']['hits']))
            pbar.update(scroll_size)        
    return rets


def parse(response):
    '''
    : trans to my need form
    '''
    try:
        return list(map(
            lambda x: {
                'trace_id': x['_source']['traceId'],
                'timestamp': x['_source']['timestamp'],
                'latency': x['_source']['duration'],
                'http_status': x['_source']['tags']['http.status_code'],
                'request_parent_id': x['_source']['parentId'] if 'parentId' in x['_source'] else 'None',
                'request_id': x['_source']['id'],
                'source': x['_source']['localEndpoint']['serviceName'],
                'http_name': x['_source']['name'],
                'target': x['_source']['name'].split('.')[0] + '.default'
            },
            response,
        ))
    except KeyError:
        print('error:', response)

        
def dump_index(index, path, es_start, es_end)->list:
    '''
    : param index: "zipkin:span-2019-08-29"
    : download es data to pickle
    '''
    rets = search_all(
            index=index, 
            body={
                  "query": {
                    "range": {
                      "timestamp": {
                        "gte": es_start,
                        "lte" : es_end
                      }
                    }
                  }
                },
        
    )
    dump_path(path,rets)
    return rets

def dump_path(path,data):
    with open(path, 'wb+') as f:
        pickle.dump(data, f)

def load_path(path):
    with open(path, 'rb') as f:
        return pickle.load(f)

def es_data(es_index, es_plk_path, es_start, es_end):
  '''
  : make es data to DataFrame form, drop some columns, delete '.default' in service's name
  '''
  requests_8_29 = dump_index(es_index, es_plk_path, es_start, es_end)
  df = pd.DataFrame(
            requests_8_29, 
            columns=[ 'trace_id',
                      'timestamp',
                      'latency',
                      'http_status',
                      'request_parent_id',
                      'request_id',
                      'source',
                      'http_name',
                      'target'] )
  df = df.drop(columns=['request_parent_id','request_id','http_name'])
  df['source'] = df['source'].apply(lambda x : ('.'+x).strip('default').strip('.'))
  df['target'] = df['target'].apply(lambda x : ('.'+x).strip('default').strip('.'))
  return df

# 下面的代码是influxdb部分
def get_db(svc, influxdb_start, influxdb_end):
    '''
    : get data from influxdb
    '''
    client = DataFrameClient('192.168.115.31',34002,'root','','aiops_metric')#初始化
    query = f'select * from "{svc}" where time >= {influxdb_start} and time<= {influxdb_end}'
    result = dict(client.query(query,chunked=False))
    result = result[svc].reset_index().rename(columns={'index':'timestamp'})
    result['timestamp'] = result.apply(lambda x : x['timestamp'].timestamp(),axis=1)
    result['service'] = svc
    return result



def dflist_2_dblist(df):
    '''
    : svc in db is larger than es, we only get name in es 
    '''
    db_list = []
    df_list = list(set(df['target']))
    for i in df_list:
        db_list.append('metric.'+i)
    return db_list


#读取influxdb所有需要的数据为一个大表
def all_db(df, influxdb_start, influxdb_end):
    '''
    : param df: es_df
    '''
    db_list=dflist_2_dblist(df)
    df=get_db(db_list[0], influxdb_start, influxdb_end)
    for i in db_list[1:]:
        df = pd.concat((df, get_db(i, influxdb_start, influxdb_end)))
    df['service']=df['service'].apply(lambda x:(x+'.').strip('metric').strip('.'))
    df = df.fillna(0)
    #cpu使用率，service，timestamp，内存使用率，内存使用量，文件系统写入速率，文件系统读取速率，网络发送速率，网络接受速率
    df.columns = ['cpu_use', 
                  'target', 
                  'timestamp', 
                  'mem_use_percent', 
                  'mem_use_amount', 
                  'file_write_rate',  
                  'file_read_rate', 
                  'net_send_rate', 
                  'net_recieve_rate']
    return df


#将es_data和db_data根据timestamp和target整合到一个列表中
class data_zh(object):

    def __init__(self, es_start, es_df, db_data):
        self.es_start = es_start
        self.df = es_df
        self.db_data = db_data        
    
    def time_to_little(self, es_timestamp):
        '''
        : es_ts is like 1567087309363872, but db_ts is 1567087200,1567087260,1567087320 and so on
        ''' 
        n = (es_timestamp - self.es_start ) // 60000000  
        return self.es_start/1000000 + n * 60

    def es_time_to_little(self, df):
        '''
        : istio is not in db, make es_ts like the form of db_ts
        '''
        df['timestamp'] = df['timestamp'].apply(self.time_to_little)#利用字典取分段函数
        df = df[~df['source'].isin(['istio-ingressgateway'])]
        return df

    def es_db_together(self):
        '''
        : es and db in a DataFrame
        '''
        df1 = self.df.copy()
        es_z = self.es_time_to_little(df1)
        result = pd.merge(es_z, self.db_data, on = ['timestamp','target'])
        result['timestamp'] = self.df['timestamp']
        return result

#将得到的表根据trace_id进行聚合，对invocation（A调用B）按end_time排序
# 每个invocation统计其(http_code, execution_time, cpu, memory)，形成一个(n_invocation, n_features)的序列

def trans_csv(df):
    '''
    : sorted end_time
    ：param df: es_db_together
    '''
    df['endtime'] = df['timestamp']+df['latency']
    grouped = df.groupby('trace_id').apply(lambda x: x.sort_values('endtime', ascending=False))
    #grouped['s_t'] = grouped['source'].str.cat(grouped['target'],sep='->')
    grouped['s_t'] = grouped['source'].str.cat(grouped['target'],sep='->')
    grouped['s_t']=grouped['s_t'].apply(lambda x: tuple(x.split('->')))
    grouped = grouped.drop(columns=['target','source','trace_id'])
    grouped.fillna(0)
    grouped.to_csv('grouped.csv') 
    return grouped

#trace每一行都整合为list，然后去掉重复的行
def trans(df):
    for i in df.columns[1:]:
        df[i] = str(list(df[i]))
    df = df.drop_duplicates(['trace_id'])
    return df

#得到按traceid整合后的invocation列表，没有error label
def trans_last(zh):
    trans_csv(zh)
    df = pd.read_csv('grouped.csv').drop(columns = 'Unnamed: 1')
    df1 = df.groupby('trace_id').apply(trans).drop(columns = 'trace_id')
    df1.to_csv('grouped1.csv')
    df2 = pd.read_csv('grouped1.csv').drop(columns = 'Unnamed: 1')
    return df2

#添加error的label
def error_label(error, df, error_start, error_end):
    outfile = df [ ( df['target'] == error ) & ( df['timestamp'] > error_start ) & (df['timestamp'] < error_end ) ] 
    error_traceid_set = set(outfile['trace_id'])
    return error_traceid_set

#change '[1,2,3]' to [1,2,3]
def int2list(int1):
    a = int1.strip('[]')
    a = [float(i) for i in a.split(',')]
    return a

def str2list(str1):
    a = eval(str1)
    a = list(map(eval,a))
    return a

# 得到最终invo，es+指标+label，以字典格式保存在plk文件中
def dict_last(zh, path, error_start, error_end, error_name, es_df):
    dfn = trans_last(zh)
    error_traceid = error_label(error_name, es_df, error_start, error_end)
    dfn['label'] = dfn.trace_id.apply(lambda x: 1 if x in error_traceid else 0)
    for i in dfn.columns[1:-2]:
        dfn[i] = dfn[i].apply(int2list)
    dfn['s_t'] = dfn['s_t'].apply(str2list)
    #dfn['s_t'] = dfn['s_t'].apply(eval)
    #for i in range(len(dfn['s_t'])):
    #    dfn['s_t'][i] = list(map(eval,dfn['s_t'][i]))
    dictn = dfn.to_dict(orient='records')
    dump_path(path, dictn)
    return dictn


@click.command('change es and db data to invocation')
@click.option('--error-start' , '-s', help="error start time like '2019-08-29 22:30'")
@click.option('--error-brief' , '-b', help="service brief name like 'basic'", default='basic')
@click.option('--output-file' , '-o', help="output file like 'invo_basic.pkl'", default='invo_basic.pkl')
def main(error_start, error_brief, output_file):
    '''
    : error_start eg: '2019-08-29 22:30'
    : error_brief eg: 'basic'
    : output_file eg: 'invo_basic.pkl"
    '''
    PATH = os.getcwd()
    day = error_start.split(' ')[0] #eg: 2019-08-29
    error_name = f'ts-{error_brief}-service' #change basic/order/seat
    DICT_PATH = f'{PATH}/{output_file}'
    es_index = f'zipkin:span-{day}'
    es_plk_path = f'{PATH}/event_title.pkl'
    es_start, es_end, influxdb_start, influxdb_end, error_start, error_end = get_time(error_start)
    es_df = es_data(es_index, es_plk_path, es_start, es_end)#get es data
    db_data = all_db(es_df, influxdb_start, influxdb_end)#get influxdb data
    es_and_db = data_zh(es_start, es_df, db_data).es_db_together() #es and db together depend on timestamp and target
    result_dict = dict_last(es_and_db, DICT_PATH, error_start, error_end, error_name, es_df)# get invocation_dict
    


if __name__ == '__main__':
    #main('2019-08-29 22:30', 'seat', '123.pkl')
    main()