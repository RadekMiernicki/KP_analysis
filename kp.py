from datetime import datetime, timedelta, time
from pytz import timezone
import pandas as pd
from os.path import join
from os import getcwd


class Hours:
    hours = zip([('0'+str(k)+':00' if k< 10 else str(k)+':00') for k in range(2,25)],
    [('0'+str(k)+':00' if k< 10 else str(k)+':00') for k in range(1,25)])
    hours = {k:v for k,v in hours}
    hours['25:00'] = '23:59'

class Holidays:
    path = '../datasets/kino_polska/holidays.csv'
    holidays = pd.read_csv(path)
    holidays.date_of_holiday = pd.to_datetime(holidays.date_of_holiday)
    holidays = holidays.set_index('date_of_holiday')
    holidays['holiday'] = 100

class Description:
    description =  {'AMR': 'Average Minute Rating: Average number of individuals who have seen a specific programme or daypart',
                'RCH': 'Reach: Number of different individuals watching at least one minute of a programme or daypart',
                'ATS': 'Average Time Spent: Average number of minutes seen by each individual who has seen the programme or daypart',
                'SHR': 'Share: Proportion of individuals viewing a specific programme or daypart compared to the total number of individuals watching TV during the same time interval'}

class CreateFeatures:
    hours: bool = False
    features: list[str] = ['dayofweek', 'month', 'quarter', 'year', 'dayofyear', 'holiday']
    @classmethod
    def date_features(cls, df:pd.DataFrame, hours=hours) -> pd.DataFrame:
        '''
        Create time series features based on time series index.
        '''
        df=df.copy()
        index = df.index
        if hours:
            df['hour'] = df.index.hour
            cls.features.append('hour')
        df['dayofweek'] = df.index.day_of_week # 0 : Monday, 6 : Sunday
        df['month'] = df.index.month
        df['quarter'] = df.index.quarter
        df['year'] = df.index.year
        df['dayofyear'] = df.index.dayofyear

        df = df.merge(Holidays.holidays['holiday'], left_on='Date', right_on=['date_of_holiday'], how='left')
        df.index = index
       
        #df.drop(columns = ['date_of_holiday'], inplace=True)
        df['holiday']=df['holiday'].fillna(0)
        df['holiday'] = df['holiday'].astype(int)
        return df
    
    # features special for xgb (different to CreteFeatures)
    @classmethod
    def create_features(cls, df: pd.DataFrame) -> pd.DataFrame:
        df['month'] = df.index.month
        df['day'] = df.index.day
        df['day_of_week'] = df.index.day_of_week
        df['day_of_yaar'] = df.index.day_of_year
        df['hour'] = df.index.hour
        df['quarter'] = df.index.quarter
        df['year'] = df.index.year - 2020
        df['date'] = pd.to_datetime(df.index.date)
        return df


class ImportData:

    dir_path = '../datasets/kino_polska/'
    prog_file = 'PROG.xls'
    monthly_file = 'MTHLY.xls' #MTHLY.xls
    daily_file = 'DAILY.xls'
    hours = Hours.hours

    @classmethod
    def monthly(cls) -> pd.DataFrame:
        monthly_file_path = join(cls.dir_path, cls.monthly_file)
        monthly_meta = pd.read_excel(monthly_file_path, nrows=1,
                                    usecols=['Target', 'Day Part group', 'Activity', 'Platform'])
        monthly = pd.read_excel(monthly_file_path, header=2,
                                dtype = {'Date\Variable': datetime})
        # setting proper data types for date time variables
        monthly.rename(columns = {'Date\Variable':'Date', 'SHR %':'SHR'}, inplace=True)
        monthly['Date'] = pd.to_datetime(monthly['Date'])
        monthly['ATS'] = monthly['ATS'].apply(cls.delta)

        return monthly

    @classmethod
    def daily(cls, localize: bool = False) -> pd.DataFrame:
        daily_file_path = join(cls.dir_path, cls.daily_file)
        daily_meta = pd.read_excel(daily_file_path, nrows=1,
                                    usecols = ['Target', 'Activity', 'Platform'])
        daily=pd.read_excel(daily_file_path, header=2,
                            dtype = {'Day Part\Variable':time, 'Date':datetime})
        daily.rename(columns={'Day Part\Variable':'DayPart', 
                                'RCH [Not cons. - TH: 0min.]':'RCH',
                                'SHR %': 'SHR'}, inplace=True)
        daily['Channel'] = pd.Categorical(daily['Channel'])

        # building DayPart with range betwenn 0:00 and 23:59                 
        daily['DP'] = daily['DayPart'].replace(cls.hours)
        # building iso TimeStamp object
        daily['TimeStamp'] = daily['Date'] +' '+ daily['DP']
        # droping unneccessary columns
        daily.drop(columns=['DP'], inplace=True)
        # change data type to datetime.Timestamp
        daily['TimeStamp'] = daily['TimeStamp'].apply(cls.to_timestamp, localize=localize)
        daily = daily.set_index('TimeStamp')
        if not localize:
            daily.index.freq = 'h'
        daily['Date'] = pd.to_datetime(daily['Date'])

        return daily

    @classmethod
    def prog(cls) -> pd.DataFrame:
        prog_file_path = join(cls.dir_path, cls.prog_file)
        prog_meta = pd.read_excel(prog_file_path, nrows=1,
                                    usecols = ['Activity', 'Platform'])
        prog = pd.read_excel(prog_file_path, header=2)
        prog.rename(columns={'SHR %':'SHR'}, inplace=True)
        return prog


    @staticmethod
    def delta (t: object, dtime=False, delimiter: str=':') -> timedelta:
        """Function setting a timedelta data type for use wiht .apply method on pd.Series object"""
        
        if dtime:
            t = datetime.strptime(str(t), f'%H{delimiter}%M')
            return time(hour = t.hour, minute=t.minute)

        t = datetime.strptime(str(t), f'%H{delimiter}%M{delimiter}%S')
        return timedelta(hours=t.hour, minutes=t.minute, seconds=t.second)

    @staticmethod
    def to_timestamp (st: str, localize: bool = False) -> pd.Timestamp:
        """Function make TimeStamp form DateTime isoformat object"""
        poland = timezone('Europe/Warsaw')
        stamp = pd.Timestamp(st)
        if stamp.time() == time(23,59):
        #if stamp.hour == 23 and stamp.minute == 59:
            stamp = stamp + timedelta(hours=1, minutes=1)
        else:
            stamp = stamp + timedelta(hours=1)
        if localize:
            return poland.localize(stamp)
        return stamp
    
class CreateCSV:
    dir_path = '../datasets/kino_polska/tableau'
    @classmethod
    def to_csv(cls, df: pd.DataFrame, name: str) -> None:
        file_name = f'{name}.csv'
        df.to_csv(join(cls.dir_path, file_name))
