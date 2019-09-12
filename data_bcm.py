import pandas as pd
import sys
sys.path.append('.')
from pathlib import Path
from tqdm import tqdm
import datetime
import time

class df_get(object):

    def __init__(self, error_start_str, normal_start_str, csv_name):
        self.error_start_str = error_start_str
        self.normal_start_str = normal_start_str
        self.csv_name = csv_name
        self.path = '/Users/wangzikai/Documents/Bizseer/TraceAnalysis/data/quoridor-data/'
        

    #datetime转换为timestamp
    def string2timestamp(self, strValue)->float:
        """
        param strValue: '2015-08-28 16:43:37'
        return: 1440751417.0
        """        
        d = datetime.datetime.strptime(strValue, "%Y-%m-%d %H:%M")
        t = d.timetuple()
        timeStamp = int(time.mktime(t))
        timeStamp = float(str(timeStamp) + str("%06d" % d.microsecond))/1000000
        return timeStamp


    def get_time(self):
        '''
        function: get time
        param error_start_str: normal start time, range is 10  min(timestamp + 600)   
        param error_start_str: error start time, range is 3  min(timestamp + 180)   eg: '2019-08-29 22:19'
        '''
        error_start           = int(self.string2timestamp(self.error_start_str))
        error_end             = error_start + 180
        normal_start          = int(self.string2timestamp(self.normal_start_str))
        normal_end            = normal_start + 600
        return normal_start, normal_end, error_start, error_end

    def df_form(self):
        '''
        return: df_normal_group: normal data groupby src,dst; df_error: error data
        '''
        normal_start, normal_end, error_start, error_end = self.get_time()
        df1 = pd.read_csv(self.path+f'{self.csv_name}.csv').drop(columns='name').rename(columns={'time':'timestamp'})
        df1.success = df1.success/df1.trans
        df2 = df1[df1['timestamp'].isin(list(range(normal_start,normal_end,10)))].drop(columns=['response'])
        df_normal_group = df2.drop(columns=['timestamp', 'trans']).groupby(['src','dst']).describe()['success']
        df_error = df1[df1['timestamp'].isin(list(range(error_start,error_end,10)))].drop(columns=['response'])
        return df_normal_group, df_error

class trans2data(object):

    def __init__(self, df_normal_group, df_error):
        self.df_normal_group = df_normal_group
        self.df_error = df_error

    def suc(self, df_err):
        '''
        避免异常时刻的service正常时刻没有出现，做一个异常处理
        '''
        try:
            return 1-abs(df_err['success'] - self.df_normal_group.loc[df_err['src']].loc[df_err['dst']]['mean'])
        except:
            return df_err['success'] 

    def trans(self, df):
        '''
        根据trans进行数量的拓展
        '''
        df1=pd.DataFrame(df.loc[[0,1]]).drop(1)
        for i in tqdm(range(1,len(df))):
            df1=df1.append([df.loc[i]]*df.loc[i]['trans'])
        return df1
    
    def get_result(self):
        df_result=pd.concat([self.df_error, pd.DataFrame(columns=['sourceip','targetip','interface','retype','recode','end'])]).reset_index()
        df_result['sourceip']= df_result['sourceip'].fillna(1)
        df_result['targetip']=df_result['targetip'].fillna(2)
        df_result['interface']=df_result['interface'].fillna('UWBZy')
        df_result.rename(columns={'index':'traceid','timestamp':'start'},inplace=True)
        df_result['start']=(df_result['start']*1000).astype('int64')
        df_result['end']=(df_result['start']+df_result['latency']).astype('int64')
        df_result['success'] = df_result.apply(self.suc,axis=1)
        df_result['recode']=df_result['success']>0.7
        df_result['recode'] = df_result['recode'].apply(lambda x: 'S' if x == True else 'E' )
        df_result['retype']=df_result['recode']
        df_result['trans']=df_result['trans'].astype('int64')
        order = ['src', 'sourceip', 'dst', 'targetip', 'traceid', 'interface', 'retype', 'recode','start','end','latency','trans']
        df_result = df_result[order]
        df_result = self.trans(df_result)
        df_result = df_result.drop(columns = 'trans')
        df_result[['start','end','latency']] = df_result[['start','end','latency']].apply(lambda x : x/1000)
        return df_result.drop(0)


def main(error_start_str, normal_start_str, csv_name, outfile):
    '''
    '2019-07-02 10:30', '2019-07-02 10:00', '0702_data' , 'data_7_2' 
    '''
    path = '/Users/wangzikai/Documents/Bizseer/TraceAnalysis/data/cmbc-chain/'
    a = df_get(error_start_str, normal_start_str, csv_name)
    df_normal_group, df_error = a.df_form()
    result = trans2data(df_normal_group, df_error).get_result()
    result.to_csv(path+f'{outfile}.csv')

if __name__ == '__main__':
    main('2019-07-02 10:30', '2019-07-02 10:00', '0702_data' , 'data_7_21' )
    #main()
