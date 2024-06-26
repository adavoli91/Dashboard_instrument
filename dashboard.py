import numpy as np
import pandas as pd
import streamlit as st
import datetime
import plotly.graph_objects as go
import decimal
import hmac
from plotly.subplots import make_subplots

def check_password():
    '''Returns `True` if the user had a correct password.'''

    def login_form():
        '''Form with widgets to collect user information'''
        with st.form('Credentials'):
            st.text_input('Username', key = 'username')
            st.text_input('Password', type = 'password', key = 'password')
            st.form_submit_button('Log in', on_click = password_entered)

    def password_entered():
        '''Checks whether a password entered by the user is correct.'''
        if ((st.session_state['username'] in st.secrets['passwords']) and
            (hmac.compare_digest(st.session_state['password'], st.secrets.passwords[st.session_state['username']]))):
            st.session_state['password_correct'] = True
            del st.session_state['password']  # Don't store the username or password.
            del st.session_state['username']
        else:
            st.session_state['password_correct'] = False

    # Return True if the username + password is validated.
    if st.session_state.get('password_correct', False):
        return True

    # Show inputs for username + password.
    login_form()
    if 'password_correct' in st.session_state:
        st.error('Login error: user not known or password incorrect')
    return False

class Dashboard:
    def __init__(self, max_rows = 250000):
        '''
        Args:
            max_rows: Maximum number of rows which can be present in a time series: if the number is exceeded, data is aggregated.
        '''
        # sidebar - choose instrument
        dict_sess = {'AD': ['17:00:00', '16:00:00'], 'ADAUSD': ['00:00:00', '23:59:00'], 'AVAXUSD': ['00:00:00', '23:59:00'], 'BP': ['17:00:00', '16:00:00'],
                     'BTC': ['17:00:00', '16:00:00'], 'BTCUSD': ['00:00:00', '23:59:00'], 'C': ['19:00:00', '13:20:00'], 'CD': ['17:00:00', '16:00:00'],
                     'CL': ['18:00:00', '17:00:00'], 'CT': ['21:00:00', '14:20:00'], 'DOGEUSD': ['00:00:00', '23:59:00'], 'EC': ['17:00:00', '16:00:00'],
                     'ES': ['17:00:00', '16:00:00'], 'ETHUSD': ['00:00:00', '23:59:00'], 'FC': ['08:30:00', '13:05:00'], 'FDAX': ['01:10:00', '22:00:00'],
                     'GC': ['18:00:00', '17:00:00'], 'HG': ['18:00:00', '17:00:00'], 'HO': ['18:00:00', '17:00:00'], 'LC': ['08:30:00', '13:05:00'],
                     'LH': ['08:30:00', '13:05:00'], 'NG': ['18:00:00', '17:00:00'], 'NQ': ['17:00:00', '16:00:00'], 'PL': ['18:00:00', '17:00:00'],
                     'RB': ['18:00:00', '17:00:00'], 'RTY': ['17:00:00', '16:00:00'], 'S': ['19:00:00', '13:20:00'], 'SB': ['03:30:00', '13:00:00'],
                     'SI': ['18:00:00', '17:00:00'], 'SOLUSD': ['00:00:00', '23:59:00'], 'THETAUSD': ['00:00:00', '23:59:00'], 'TY': ['17:00:00', '16:00:00'],
                     'US': ['17:00:00', '16:00:00'], 'VX': ['17:00:00', '16:00:00'], 'XRPUSD': ['00:00:00', '23:59:00'], 'YM': ['17:00:00', '16:00:00']}
        dict_settlement_hour = {'AD': '14:00:00', 'BP': '14:00:00', 'BTC': '15:00:00', 'C': '13:15:00', 'CD': '14:00:00', 'CL': '14:30:00',
                                'EC': '14:00:00', 'ES': '15:00:00', 'FC': '13:00:00', 'FDAX': '22:00:00', 'GC': '13:30:00', 'HG': '13:00:00', 'HO': '14:30:00',
                                'LC': '13:00:00', 'LH': '13:00:00', 'NG': '14:30:00', 'NQ': '15:00:00', 'PL': '13:05:00', 'RB': '14:30:00', 'RTY': '15:00:00',
                                'S': '13:15:00', 'SI': '13:25:00', 'TY': '15:00:00', 'US': '14:00:00', 'VX': '15:00:00', 'YM': '14:00:00'}
        dict_rth = {'AD': ['07:20:00', '14:00:00'], 'BP': ['07:20:00', '14:00:00'], 'C': ['08:30:00', '13:20:00'], 'CD': ['07:20:00', '14:00:00'],
                    'CL': ['09:00:00', '14:30:00'], 'EC': ['07:20:00', '14:00:00'], 'ES': ['08:30:00', '15:15:00'], 'FDAX': ['08:00:00', '22:00:00'],
                    'GC': ['08:20:00', '13:30:00'], 'HG': ['08:10:00', '13:00:00'], 'HO': ['09:00:00', '14:30:00'], 'NG': ['09:00:00', '14:30:00'],
                    'NQ': ['08:30:00', '15:15:00'], 'PL': ['08:20:00', '13:05:00'], 'RB': ['09:00:00', '14:30:00'], 'RTY': ['08:30:00', '15:15:00'],
                    'S': ['08:30:00', '13:20:00'], 'SI': ['08:25:00', '13:25:00'], 'TY': ['08:30:00', '15:15:00'],
                    'US': ['08:30:00', '15:15:00'], 'VX': ['08:30:00', '15:15:00'], 'YM': ['08:30:00', '15:15:00']}
        dict_month = {'Jan': 1, 'Feb': 2, 'Mar': 3, 'Apr': 4, 'May': 5, 'Jun': 6, 'Jul': 7, 'Aug': 8, 'Sep': 9, 'Oct': 10, 'Nov': 11, 'Dec': 12}
        dict_day_of_week = {'Mon': 0, 'Tue': 1, 'Wed': 2, 'Thu': 3, 'Fri': 4, 'Sat': 5, 'Sun': 6}
        #
        list_instr = list(dict_sess.keys())
        list_instr = [i for i in list_instr if 'USD' not in i]
        instrument = st.sidebar.selectbox(label = 'Instrument:', options = list_instr)
        # sidebar - choose timeframe
        timeframe = st.sidebar.radio(label = 'Timeframe:', options = ['1m', '5m', '15m', '30m', '60m', '120m', '240m', '480m', 'Daily', 'Weekly'],
                                     horizontal = True)
        # sidebar - choose plot type
        plot_type = st.sidebar.radio(label = 'Plot type:', options = ['Lines', 'Bars'], horizontal = True)
        #
        self.dict_sess = dict_sess
        self.dict_settlement_hour = dict_settlement_hour
        self.dict_rth = dict_rth
        self.dict_month = dict_month
        self.dict_day_of_week = dict_day_of_week
        self.instrument = instrument
        self.max_rows = max_rows
        self.timeframe = timeframe
        self.plot_type = plot_type
        #
        self._get_dates_filter()
        self._get_month_filter()
        self._get_day_of_month_filter()
        self._get_day_of_week_filter()
        self._get_data()
        self._get_time_filter()
        #
        self._select_number_of_metrics()
        self._select_metric()
        self._select_group_strategy()
        self._select_split_in_periods()
        self._select_group_function()
        self._select_unit()
        self._highlight_trading_sessions_rth()
        if self.n_metrics == 1:
            self._get_tops_bottoms()

    def _get_dates_filter(self):
        '''
        Function to create the filter for dates.

        Args: None.

        Returns: None.
        '''
        # sidebar - choose the way to filter dates
        filt_date = st.sidebar.selectbox(label = 'Filter date by: ', options = ['Slider', 'Calendar'])
        filter_date_start = datetime.datetime.strptime('2010-01-01', '%Y-%m-%d')
        filter_date_end = datetime.datetime.strptime('2050-01-01', '%Y-%m-%d')
        # get current month
        curr_date = datetime.datetime.now()
        curr_month_date = datetime.datetime.strptime(f'{curr_date.year}-{str(curr_date.month).zfill(2)}-01', '%Y-%m-%d')
        # if in the future from now, set the last date to the last day of the past month
        if filter_date_end > curr_date:
            filter_date_end = curr_month_date - datetime.timedelta(days = 1)
        # sidebar - filter date with slider
        if filt_date == 'Slider':
            filter_date = st.sidebar.slider(label = 'Date range', min_value = filter_date_start, max_value = filter_date_end,
                                            value = [filter_date_start, filter_date_end])
        # sidebar - filter date with calendar
        elif filt_date == 'Calendar':
            filter_date = st.sidebar.date_input(label = 'Date range', min_value = filter_date_start, max_value = filter_date_end,
                                                value = [filter_date_start, filter_date_end])
        # no date filter
        else:
            filter_date = [filter_date_start, filter_date_end]
        #
        self.date_start = filter_date[0].strftime('%Y-%m-%d')
        self.date_end = filter_date[1].strftime('%Y-%m-%d')

    def _get_month_filter(self):
        '''
        Function to create the filter for months.

        Args: None.

        Returns: None.
        '''
        # sidebar - filter month
        filt_month = st.sidebar.multiselect(label = 'Months to exclude:',
                                            options = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'])
        self.filt_month = filt_month

    def _get_day_of_month_filter(self):
        '''
        Function to create the filter for the days of month.

        Args: None.

        Returns: None.
        '''
        # sidebar - filter day of month
        filt_day_month = st.sidebar.multiselect(label = 'Days of month to exclude:', options = range(1, 32))
        self.filt_day_month = filt_day_month

    def _get_day_of_week_filter(self):
        '''
        Function to create the filter for the days of week.

        Args: None.

        Returns: None.
        '''
        # sidebar - filter day of week
        filt_day_week = st.sidebar.multiselect(label = 'Days of week to exclude:', options = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'])
        self.filt_day_week = filt_day_week

    def _get_data(self):
        '''
        Function to import data.

        Args: None.

        Returns: None.
        '''
        df = pd.read_pickle(f'./data/data_{self.instrument}.pickle.gz')
        df['date'] = pd.to_datetime(df['date'])
        # add a session counter
        df.loc[df['session_start'] == True, 'n_sess'] = range(df['session_start'].sum())
        df['n_sess'] = df['n_sess'].ffill()
        df = df[~df['n_sess'].isnull()].reset_index(drop = True)
        #
        self.df = df

    def _get_time_filter(self):
        '''
        Function to create the filter for times.

        Args: None.

        Returns: None.
        '''
        df = self.df.copy()
        timeframe = self.timeframe
        self.filter_time = [None, None]
        #
        if timeframe in ['1m', '5m', '15m', '30m', '60m', '120m', '240m', '480m']:
            sess_start, sess_end = self.dict_sess[self.instrument]
            # compute time ranges
            range_times = []
            curr_time = datetime.datetime.strptime(sess_start, '%H:%M:%S')
            if (sess_start == '00:00:00') and (sess_end == '23:59:00'):
                time_end = datetime.datetime.strptime('23:59:00', '%H:%M:%S')
            elif eval(sess_start.split(':')[0].lstrip('0')) < eval(sess_end.split(':')[0].lstrip('0')):
                time_end = datetime.datetime.strptime(sess_end, '%H:%M:%S')
            else:
                time_end = datetime.datetime.strptime(sess_end, '%H:%M:%S') + datetime.timedelta(days = 1)
            while curr_time <= time_end:
                range_times.append(curr_time.strftime("%H:%M:%S"))
                curr_time += pd.Timedelta(5, unit = 'min')
            if sess_end == '23:59:00':
                range_times.append('23:59:59')
            # sidebar - filter time
            self.filter_time = st.sidebar.select_slider(label = 'Time range:', options = range_times, value = [range_times[0], range_times[-1]])
            #
            self.sess_start = sess_start
            self.sess_end = sess_end

    def _select_number_of_metrics(self):
        '''
        Function to select the number of metrics to use.

        Args: None.

        Returns: None.
        '''
        self.n_metrics = 1
        if self.timeframe in ['1m', '5m', '15m', '30m', '60m', '120m', '240m', '480m']:
            self.n_metrics = st.sidebar.radio(label = 'Number of metrics:', options = [1, 2], horizontal = True)

    def _select_metric(self):
        '''
        Function to select the metric to use.

        Args: None.

        Returns: None.
        '''
        if self.timeframe in ['1m', '5m', '15m', '30m', '60m', '120m', '240m', '480m']:
            if self.n_metrics == 1:
                self.metric = st.sidebar.radio(label = 'Metric:', options = ['Close', 'Delta close', 'Body', 'Range', 'Open-high', 'Open-low', 'Num highs',
                                                                            'Num lows', 'Num highs or lows','Volume'],
                                            horizontal = True)
            else:
                self.metric = st.sidebar.multiselect(label = 'Metrics (choose 2):', options = ['Close', 'Delta close', 'Body', 'Range', 'Open-high',
                                                                                               'Open-low', 'Num highs', 'Num lows', 'Num highs or lows',
                                                                                               'Volume'])
                self.metric = self.metric[:2]
        else:
            if self.n_metrics == 1:
                self.metric = st.sidebar.radio(label = 'Metric:', options = ['Close', 'Body', 'Range', 'Open-high', 'Open-low', 'Volume'],
                                            horizontal = True)
            else:
                self.metric = st.sidebar.multiselect(label = 'Metrics (choose 2):', options = ['Close', 'Body', 'Range', 'Open-high', 'Open-low', 'Volume'])
                self.metric = self.metric[:2]

    def _select_group_strategy(self):
        '''
        Function to select the strategy to use in grouping.

        Args: None.

        Returns: None.
        '''
        if self.timeframe in ['1m', '5m', '15m', '30m', '60m', '120m', '240m', '480m']:
            self.group_by = st.sidebar.radio(label = 'Group by:', options = [None, 'Time', 'Day of week + time', 'Day of month + time', 'Month + time',
                                                                             'Month + day of month + time', 'History', 'Day of week + history',
                                                                             'Day of month + history', 'Month + history'],
                                             horizontal = True)
        else:
            self.group_by = None

    def _select_split_in_periods(self):
        '''
        Function to decide whether to show split outputs by (periods of) years.

        Args: None.

        Returns: None.
        '''
        self.split_in_periods = 'No'
        if self.group_by is not None:
            self.split_in_periods = st.sidebar.radio(label = 'Split in periods:', options = ['No', 'By year', 'By two years', 'By three years'],
                                                     horizontal = True)

    def _select_group_function(self):
        '''
        Function to select the function to use in grouping.

        Args: None.

        Returns: None.
        '''
        self.group_function = st.sidebar.radio(label = 'Grouping function:', options = ['Mean', 'Median', 'Sum', 'Cumsum', 'Count', 'Std'],
                                               horizontal = True)

    def _select_unit(self):
        '''
        Function to select the unit to use.

        Args: None.

        Returns: None.
        '''
        unit = st.sidebar.radio(label = 'Unit:', options = ['Points', '$'], horizontal = True)
        self.unit = unit.lower()
                
    def _highlight_trading_sessions_rth(self):
        '''
        Function to decide wheter to highlight trading sessions or regular trading session, when `Time` is the variable on the x-axis.

        Args: None.

        Returns: None.
        '''
        self._plot_trading_sessions = None
        self._plot_rth = None
        #
        if self.timeframe in ['1m', '5m', '15m', '30m', '60m', '120m', '240m', '480m']:
            sess_start, sess_end = self.dict_sess[self.instrument]
            if (((sess_start == '17:00:00') and (sess_end == '16:00:00')) or ((sess_start == '18:00:00') and (sess_end == '17:00:00')) or
                (self.instrument == 'FDAX')) and (self.timeframe in ['1m', '5m', '15m', '30m', '60m']):
                self._plot_trading_sessions = st.sidebar.radio(label = 'Highlight trading sessions', options = ['Yes', 'No'], horizontal = True)
            if self.instrument in self.dict_rth.keys():
                self._plot_rth = st.sidebar.radio(label = 'Highlight regular trading hours', options = ['No', 'Yes'], horizontal = True)
            if self._plot_trading_sessions == 'Yes':
                self._plot_rth = 'No'

    def _get_tops_bottoms(self):
        '''
        Function to decide wheter to plot highest and lowest values of the selected metric.

        Args: None.

        Returns: None.
        '''
        self.plot_tops_bottoms = 'No'
        if self.timeframe in ['1m', '5m', '15m', '30m', '60m', '120m', '240m', '480m']:
            self.plot_tops_bottoms = st.sidebar.radio(label = 'Plot tops and bottoms', options = ['No', 'Yes'], horizontal = True)

    def _filter_dates(self):
        '''
        Function to filter dates.

        Args: None.

        Returns: None.
        '''
        df = self.df.copy()
        date_start = self.date_start
        date_end = self.date_end
        #
        self.df = df[(df['date'] >= date_start) & (df['date'] <= date_end)].reset_index(drop = True)

    def _filter_month(self):
        '''
        Function to filter months.

        Args: None.

        Returns: None.
        '''
        df = self.df.copy()
        if len(self.filt_month) > 0:
            dict_month = self.dict_month
            self.filt_month = [dict_month[i] for i in self.filt_month]
            #
            df['month'] = df['date'].dt.month
            #
            self.df = df[~df['month'].isin(self.filt_month)].reset_index(drop = True)

    def _filter_day_of_month(self):
        '''
        Function to filter days of month.

        Args: None.

        Returns: None.
        '''
        df = self.df.copy()
        if len(self.filt_day_month) > 0:
            #
            df['day_of_month'] = df['date'].dt.day
            #
            self.df = df[~df['day_of_month'].isin(self.filt_day_month)].reset_index(drop = True)

    def _filter_day_of_week(self):
        '''
        Function to filter days of week.

        Args: None.

        Returns: None.
        '''
        df = self.df.copy()
        if len(self.filt_day_week) > 0:
            dict_day_of_week = self.dict_day_of_week
            self.filt_day_week = [dict_day_of_week[i] for i in self.filt_day_week]
            # the weekday indicates the day of the week when the session starts
            df['weekday'] = df['date'].dt.weekday
            df = df.drop('weekday', axis = 1).merge(df.loc[df['session_start'] == True, ['date', 'weekday']], on = 'date', how = 'left')
            df['weekday'] = df['weekday'].ffill()
            df = df[~df['weekday'].isnull()].reset_index(drop = True)
            #
            self.df = df[~df['weekday'].isin(self.filt_day_week)].reset_index(drop = True)

    def _filter_times(self):
        '''
        Function to filter times.

        Args: None.

        Returns: None.
        '''
        df = self.df.copy()
        #
        if (self.filter_time[0] is not None) and (self.filter_time[1] is not None):
            df['time'] = df['date'].dt.time
            # define a fake start session as the first time after the initial filtering time
            df['date_only'] = df['date'].dt.date
            df_temp = df[df['time'] >= pd.to_datetime(self.filter_time[0]).time()].groupby('date_only').agg({'date': 'first'}).reset_index()
            df_temp['session_start_fake'] = 1
            df = df.merge(df_temp, on = ['date_only', 'date'], how = 'left')
            #
            if self.filter_time[0] < self.filter_time[1]:
                df = df[(df['time'] >= pd.to_datetime(self.filter_time[0]).time()) &
                        (df['time'] <= pd.to_datetime(self.filter_time[1]).time())].drop('time', axis = 1).reset_index(drop = True)
            else:
                df = df[(df['time'] >= pd.to_datetime(self.filter_time[0]).time()) |
                        (df['time'] <= pd.to_datetime(self.filter_time[1]).time())].drop('time', axis = 1).reset_index(drop = True)
            #
            df['session_start_fake'] = df['session_start_fake'].fillna(0).astype(bool)
            df = df.drop(['date_only', 'session_start'], axis = 1).rename(columns = {'session_start_fake': 'session_start'})
            #
            self.df = df

    def _group_to_timeframe(self):
        '''
        Function to group data according to the chosen timeframe.

        Args: None.

        Returns: None.
        '''
        df = self.df.copy()
        timeframe = self.timeframe
        # intraday timeframe
        if timeframe in ['5m', '15m', '30m', '60m', '120m', '240m', '480m']:
            timeframe = timeframe.replace('m', 'min')
            # round time to appropriate (according to timeframe) bar
            df['date'] = df['date'].dt.ceil(timeframe)
            df = df.groupby('date').agg({'session_start': 'sum', 'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last', 'bpv': 'first',
                                         'vol': 'sum', 'n_sess': 'max'}).reset_index()
        # daily timeframe
        if timeframe == 'Daily':
            df['date'] = df['date'].dt.date
            df = df.groupby('date').agg({'session_start': 'sum', 'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last', 'bpv': 'first',
                                         'vol': 'sum', 'n_sess': 'max'}).reset_index()
            df['date'] = pd.to_datetime(df['date'], format = '%Y-%m-%d')
        # weekly timeframe
        if timeframe == 'Weekly':
            df = pd.concat((df, df['date'].dt.isocalendar()), axis = 1)
            df = df.groupby(['year', 'week']).agg({'date': 'max', 'session_start': 'sum', 'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last',
                                                   'bpv': 'first', 'vol': 'sum'}).reset_index()
        #
        self.df = df
        
    def _compute_metric(self):
        '''
        Function to compute the metric.

        Args: None.

        Returns: None.
        '''
        df = self.df.copy()
        #
        def _define_metric(df, metric):
            # close
            if metric == 'Close':
                df['metric'] = df['close']
            # difference between consecutive closes
            elif metric == 'Delta close':
                df['metric'] = df['close'] - df['close'].shift(1)
                # set `delta` to 0 when the session changes
                if self.timeframe in ['1m', '5m', '15m', '30m', '60m', '120m', '240m', '480m']:
                    df.loc[df['n_sess'] != df['n_sess'].shift(1), 'metric'] = 0
            # body
            elif metric == 'Body':
                df['metric'] = df['close'] - df['open']
            # range
            elif metric == 'Range':
                df['metric'] = df['high'] - df['low']
            # difference open-high
            elif metric == 'Open-high':
                df['metric'] = df['high'] - df['open']
            # difference open-low
            elif metric == 'Open-low':
                df['metric'] = df['open'] - df['low']
            # number of times there is a maximum
            elif metric == 'Num highs':
                df['date_temp'] = df['date'].dt.date
                df.loc[df.groupby('date_temp').agg({'high': 'idxmax'})['high'].values, 'high_sess'] = 1
                df['high_sess'] = df['high_sess'].fillna(0).astype(int)
                df['metric'] = df['high_sess']
            # number of times there is a minimum
            elif metric == 'Num lows':
                df['date_temp'] = df['date'].dt.date
                df.loc[df.groupby('date_temp').agg({'low': 'idxmin'})['low'].values, 'low_sess'] = 1
                df['low_sess'] = df['low_sess'].fillna(0).astype(int)
                df['metric'] = df['low_sess']
            # number of times there is a maximum/minimum
            elif metric == 'Num highs or lows':
                df['date_temp'] = df['date'].dt.date
                df.loc[df.groupby('date_temp').agg({'high': 'idxmax'})['high'].values, 'high_sess'] = 1
                df.loc[df.groupby('date_temp').agg({'low': 'idxmin'})['low'].values, 'low_sess'] = 1
                df['high_sess'] = df['high_sess'].fillna(0).astype(int)
                df['low_sess'] = df['low_sess'].fillna(0).astype(int)
                df['metric'] = df['high_sess'] + df['low_sess']
            # volume
            elif metric == 'Volume':
                df['metric'] = df['vol']
            #
            return df['metric'].values
        
        # get metric
        if type(self.metric) == str:
            df['metric'] = _define_metric(df, self.metric)
        else:
            df['metric_1'] = _define_metric(df, self.metric[0])
            df['metric_2'] = _define_metric(df, self.metric[1])
        # rescale metric by BPV
        if self.unit == '$':
            if (type(self.metric) == str) and (self.metric in ['Close', 'Delta close', 'Body', 'Range', 'Open-high', 'Open-low']):
                df['metric'] *= df['bpv']
            if type(self.metric) == list:
                if self.metric[0] in ['Close', 'Delta close', 'Body', 'Range', 'Open-high', 'Open-low']:
                    df['metric_1'] *= df['bpv']
                if self.metric[1] in ['Close', 'Delta close', 'Body', 'Range', 'Open-high', 'Open-low']:
                    df['metric_2'] *= df['bpv']
        #
        self.df = df

    def _add_split_period(self):
        '''
        Function to split the years in groups.

        Args: None.

        Returns: None.
        '''
        df = self.df.copy()
        df['period'] = ''
        #
        if self.split_in_periods != 'No':
            df['year'] = df['date'].dt.year
            list_years = np.arange(df['year'].min(), df['year'].max() + 1)
            #
            if self.split_in_periods == 'By year':
                st.write('The results are splitted by year.')
                df['period'] = df['year'].astype(str)
            else:
                len_list_years = list_years.shape[0]
                #
                if self.split_in_periods == 'By two years':
                    dim_split = 2
                if self.split_in_periods == 'By three years':
                    dim_split = 3
                # group years
                if len_list_years%dim_split != 0:
                    list_years = list_years[len_list_years%dim_split:].reshape(-1, dim_split)
                else:
                    list_years = list_years.reshape(-1, dim_split)
                # new labels for groups
                list_labels = [f'{i[0]}-{i[-1]}' for i in list_years]
                st.write('The selected periods are: ' + ', '.join(list_labels) + '.')
                # apply grouping label
                dict_repl = {list_years[i][j]: list_labels[i] for i in range(list_years.shape[0]) for j in range(list_years.shape[1])}
                df['period'] = df['year'].replace(dict_repl)
            # remove years which are not in the list
            df = df[df['year'] >= list_years.min()].reset_index(drop = True)
        self.df = df
        
    def _group_data(self):
        '''
        Function to group data according to chosen strategy.

        Args: None.

        Returns: None.
        '''
        df = self.df.copy()
        self.col_x = 'date'
        self.format_x = '%Y-%m-%d %H:%M:%S'
        self.col_color = None
        #
        if self.group_by is not None:
            # shift time so that session begin corresponds to 00:00:00. It will be fixed later in the code
            df['time'] = (df['date'] - pd.Timedelta(eval(self.sess_start.split(':')[0].lstrip('0')), unit = 'h')).dt.time.astype(str)
            # weekday. notice: the weekday indicates the day of the week when the session starts
            df['weekday'] = df['date'].dt.weekday
            df = df.drop('weekday', axis = 1).merge(df.loc[df['session_start'] == True, ['date', 'weekday']], on = 'date', how = 'left')
            df['weekday'] = df['weekday'].ffill()
            df = df[~df['weekday'].isnull()].reset_index(drop = True)
            df['weekday'] = df['weekday'].astype(int).replace({value: key for key, value in self.dict_day_of_week.items()})
            # day of month
            df['day_of_month'] = df['date'].dt.day.astype(str)
            # month
            df['month'] = df['date'].dt.month.replace({value: key for key, value in self.dict_month.items()})
            # all history
            df['history'] = df['date'].astype(str)
            # define grouping criterion
            if self.group_by == 'Time':
                # df.to_pickle('./aa.pickle.gz')
                if self.split_in_periods == 'No':
                    self.group_cols = 'time'
                    self.col_color = None
                else:
                    self.group_cols = ['period', 'time']
                    self.col_color = 'period'
                self.col_x = 'time'
                self.format_x = '%H:%M:%S'
            if self.group_by == 'Day of week + time':
                if self.split_in_periods == 'No':
                    self.group_cols = ['weekday', 'time']
                    self.col_color = 'weekday'
                else:
                    self.group_cols = ['weekday', 'period', 'time']
                    df['period'] = df['weekday'] + ' - ' + df['period']
                    self.col_color = 'period'
                self.col_x = 'time'
                self.format_x = '%H:%M:%S'
                st.write('Notice: the day of week has to be interpreted as the day of the week when the session starts.')
            if self.group_by == 'Day of month + time':
                if self.split_in_periods == 'No':
                    self.group_cols = ['day_of_month', 'time']
                    self.col_color = 'day_of_month'
                else:
                    self.group_cols = ['day_of_month', 'period', 'time']
                    df['period'] = df['day_of_month'] + ' - ' + df['period']
                    self.col_color = 'period'
                self.col_x = 'time'
                self.format_x = '%H:%M:%S'
            if self.group_by == 'Month + time':
                if self.split_in_periods == 'No':
                    self.group_cols = ['month', 'time']
                    self.col_color = 'month'
                else:
                    self.group_cols = ['month', 'period', 'time']
                    df['period'] = df['month'] + ' - ' + df['period']
                    self.col_color = 'period'
                self.col_x = 'time'
                self.format_x = '%H:%M:%S'
            if self.group_by == 'Month + day of month + time':
                if self.split_in_periods == 'No':
                    self.group_cols = ['month', 'day_of_month', 'time']
                    self.col_color = 'month'
                else:
                    self.group_cols = ['month', 'period', 'day_of_month', 'time']
                    df['period'] = df['month'] + ' - ' + df['period']
                    self.col_color = 'period'
                self.col_x = 'day_of_month_time'
                self.format_x = '%Y-%m-%d %H:%M:%S'
            if self.group_by == 'History':
                if self.split_in_periods == 'No':
                    self.group_cols = 'history'
                    self.col_color = None
                else:
                    self.group_cols = ['period', 'history']
                    self.col_color = 'period'
                self.col_x = 'history'
                self.format_x = '%Y-%m-%d %H:%M:%S'
            if self.group_by == 'Day of week + history':
                if self.split_in_periods == 'No':
                    self.group_cols = ['weekday', 'history']
                    self.col_color = 'weekday'
                else:
                    self.group_cols = ['weekday', 'period', 'history']
                    df['period'] = df['weekday'] + ' - ' + df['period']
                    self.col_color = 'period'
                self.col_x = 'history'
                self.format_x = '%Y-%m-%d %H:%M:%S'
                st.write('Notice: the day of week has to be interpreted as the day of the week when the session starts.')
            if self.group_by == 'Day of month + history':
                if self.split_in_periods == 'No':
                    self.group_cols = ['day_of_month', 'history']
                    self.col_color = 'day_of_month'
                else:
                    self.group_cols = ['day_of_month', 'period', 'history']
                    df['period'] = df['day_of_month'] + ' - ' + df['period']
                    self.col_color = 'period'
                self.col_x = 'history'
                self.format_x = '%Y-%m-%d %H:%M:%S'
            if self.group_by == 'Month + history':
                if self.split_in_periods == 'No':
                    self.group_cols = ['month', 'history']
                    self.col_color = 'month'
                else:
                    self.group_cols = ['month', 'period', 'history']
                    df['period'] = df['month'] + ' - ' + df['period']
                    self.col_color = 'period'
                self.col_x = 'history'
                self.format_x = '%Y-%m-%d %H:%M:%S'
            # group data: 1 metric
            if type(self.metric) == str:
                # the function is not 'cumsum'
                if self.group_function != 'Cumsum':
                    # for counts of highs/lows, use 'sum' instead of 'mean'
                    if (self.metric in ['Num highs', 'Num lows', 'Num highs or lows']) and (self.group_function == 'Mean'):
                        self.group_function = 'Sum'
                    #
                    df = df.groupby(self.group_cols).agg({**{'metric': self.group_function.lower()},
                                                          **{'date': 'max', 'session_start': 'sum', 'open': 'first', 'high': 'max', 'low': 'min',
                                                             'close': 'last', 'bpv': 'first', 'vol': 'sum'}}).reset_index()
                # the function is 'cumsum'
                else:
                    # no multiple breakdown
                    if self.col_color is None:
                        df = df.groupby(self.group_cols).agg({**{'metric': 'mean'},
                                                              **{'date': 'max', 'session_start': 'sum', 'open': 'first', 'high': 'max', 'low': 'min',
                                                                 'close': 'last', 'bpv': 'first', 'vol': 'sum'}}).reset_index()
                        df['metric'] = df['metric'].cumsum()
                    # multiple breakdown
                    else:
                        df = df.groupby(self.group_cols).agg({**{'metric': 'mean'},
                                                              **{'date': 'max', 'session_start': 'sum', 'open': 'first', 'high': 'max', 'low': 'min',
                                                                 'close': 'last', 'bpv': 'first', 'vol': 'sum'}})
                        df = df.drop('metric', axis = 1).merge(df.groupby(self.col_color).agg({'metric': 'cumsum'}), left_index = True,
                                                               right_index = True).reset_index()
            # group data: 2 metrics
            else:
                # the function is not 'cumsum'
                if self.group_function != 'Cumsum':
                    # for counts of highs/lows, use 'sum' instead of 'mean'
                    if ((self.metric[0] in ['Num highs', 'Num lows', 'Num highs or lows']) and
                        (self.metric[1] in ['Num highs', 'Num lows', 'Num highs or lows']) and (self.group_function == 'Mean')):
                        self.group_function = 'Sum'
                    #
                    if self.metric[0] in ['Num highs', 'Num lows', 'Num highs or lows']:
                        df['metric_1'], df['metric_2'] = df['metric_2'], df['metric_1']
                        self.metric = self.metric[::-1]
                    #
                    df_temp = df.groupby(self.group_cols).agg({**{'metric_1': self.group_function.lower()},
                                                               **{'date': 'max', 'session_start': 'sum', 'open': 'first', 'high': 'max', 'low': 'min',
                                                                  'close': 'last', 'bpv': 'first', 'vol': 'sum'}}).reset_index()
                    # for counts of highs/lows, use 'sum' instead of 'mean'
                    if (self.metric[1] in ['Num highs', 'Num lows', 'Num highs or lows']) and (self.group_function == 'Mean'):
                        self.group_function = 'Sum'
                    #
                    df = df.groupby(self.group_cols).agg({'metric_2': self.group_function.lower()}).reset_index()
                    df = df.merge(df_temp, on = self.group_cols, how = 'left')
                # the function is 'cumsum'
                else:
                    # no multiple breakdown
                    if self.col_color is None:
                        df_temp = df.groupby(self.group_cols).agg({**{'metric_1': 'mean'},
                                                                   **{'date': 'max', 'session_start': 'sum', 'open': 'first', 'high': 'max', 'low': 'min',
                                                                      'close': 'last', 'bpv': 'first', 'vol': 'sum'}}).reset_index()
                        df = df.groupby(self.group_cols).agg({'metric_2': 'mean'}).reset_index()
                        df = df.merge(df_temp, on = self.group_cols, how = 'left')
                        df['metric_1'] = df['metric_1'].cumsum()
                        df['metric_2'] = df['metric_2'].cumsum()
                    # multiple breakdown
                    else:
                        df_temp = df.groupby(self.group_cols).agg({**{'metric_1': 'mean'},
                                                                   **{'date': 'max', 'session_start': 'sum', 'open': 'first', 'high': 'max', 'low': 'min',
                                                                      'close': 'last', 'bpv': 'first', 'vol': 'sum'}})
                        df = df.groupby(self.group_cols).agg({'metric_2': 'mean'})
                        df = df.merge(df_temp, how = 'left', left_index = True, right_index = True)
                        df = df.drop('metric_1', axis = 1).merge(df.groupby(self.col_color).agg({'metric_1': 'cumsum'}), left_index = True,
                                                                 right_index = True).reset_index()
                        df = df.drop('metric_2', axis = 1).merge(df.groupby(self.col_color).agg({'metric_2': 'cumsum'}), left_index = True,
                                                                 right_index = True).reset_index()
            # if `weekday` or `month` are grouping keys, replace them with the corresponding string value
            # if 'weekday' in df.columns:
                # df['weekday'] = df['weekday'].replace({value: key for key, value in self.dict_day_of_week.items()})
                # df['weekday'] = df['weekday'].apply(lambda x: {str(value): key for key, value in self.dict_day_of_week.items()}[x])
            # if 'month' in df.columns:
            #     df['month'] = df['month'].replace({value: key for key, value in self.dict_month.items()})
                # df['month'] = df['month'].apply(lambda x: {value: key for key, value in self.dict_month.items()}[x])
            # group by month, day of month and time (i.e., to study seasonalities)
            if self.col_x == 'day_of_month_time':        
                df['day of month'] = pd.to_datetime('2000-01-' + df['day_of_month'].str.zfill(2) + ' ' + df['time'])
                df = df.drop('time', axis = 1)
                self.col_x = 'day of month'
            #
            self.df = df

    def _adjust_timeframe(self):
        '''
        Function to adjust the timeframe is the time series is too long (controlled by the `max_rows` class input parameter).

        Args: None.

        Returns: None.
        '''
        df = self.df.copy()
        if self.col_color is None:
            n_rows = df.shape[0]
        else:
            n_rows = df.groupby(self.col_color).agg({'close': 'count'})['close'].max()
        #
        if n_rows > self.max_rows:
            # original timeframe is 1 minute
            if self.timeframe == '1m':
                if self.max_rows > n_rows/5:
                    new_timeframe = 5
                elif (n_rows/15 < self.max_rows < n_rows/5):
                    new_timeframe = 15
                elif (n_rows/30 < self.max_rows < n_rows/15):
                    new_timeframe = 30
                else:
                    new_timeframe = 60
            # original timeframe is 5 minutes
            if self.timeframe == '5m':
                if self.max_rows > n_rows/3:
                    new_timeframe = 15
                elif (n_rows/6 < self.max_rows < n_rows/3):
                    new_timeframe = 30
                else:
                    new_timeframe = 60
            # original timeframe is 15 minutes
            if self.timeframe == '15m':
                if self.max_rows > n_rows/2:
                    new_timeframe = 30
                else:
                    new_timeframe = 60
            # original timeframe is 30 minutes
            if self.timeframe == '30m':
                if self.max_rows > n_rows/2:
                    new_timeframe = 60
            # round dates and times to the new timeframe
            if new_timeframe == 5:
                df['date'] = df['date'].dt.ceil('5min')
            if new_timeframe == 15:
                df['date'] = df['date'].dt.ceil('15min')
            if new_timeframe == 30:
                df['date'] = df['date'].dt.ceil('30min')
            if new_timeframe == 60:
                df['date'] = df['date'].dt.ceil('1h')
            # group data according to the new timeframe
            st.write(f'Warning: the series was too long; the timeframe has been automatically changed to {new_timeframe} minutes.')
            df = df.groupby('date').agg({'session_start': 'sum', 'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last', 'bpv': 'first',
                                         'vol': 'sum'}).reset_index()
            self.df = df
            self._compute_metric()
            self._group_data()

    def _fix_missing_dates(self):
        '''
        Function to fix possible missing dates: if a date or time is missing, it is added, in order to properly plot the results.

        Args: None.

        Returns: None.
        '''
        df = self.df.copy()
        #
        if self.col_color is None:
            df = df.sort_values(by = self.col_x).reset_index(drop = True)
        else:
            combinations = np.meshgrid(df[self.col_x].unique(), df[self.col_color].unique())
            df_temp = pd.DataFrame({self.col_x: combinations[0].ravel(), self.col_color: combinations[1].ravel()})
            df = df_temp.merge(df, on = [self.col_x, self.col_color], how = 'left')
            df = df.sort_values(by = [self.col_x, self.col_color]).reset_index(drop = True)
        #
        self.df = df

    def _plot_1_metric(self):
        '''
        Function to plot the main chart.

        Args: None.

        Returns: None.
        '''
        df = self.df.copy()
        # number of digits to use
        n_digits = 0
        if np.diff(df['Metric'].drop_duplicates().sort_values().values[:2])[0] > 0:
            n_digits = len(decimal.Decimal(round(1/np.diff(df['Metric'].drop_duplicates().sort_values().values[:2])[0])).as_tuple().digits)
        # add unit to y axis label
        label_y = self.metric
        if label_y in ['Close', 'Delta close', 'Body', 'Range', 'Open-high', 'Open-low']:
            label_y += f' [{self.unit}]'
        # shift time back to original values: this way, the first row corresponds to session start
        if dashboard.col_x == 'Time':
            df['Time'] = (pd.to_datetime('2000-01-01 ' + df['Time']) +
                          pd.Timedelta(eval(self.sess_start.split(':')[0].lstrip('0')), unit = 'h')).dt.time
        #
        figure = go.Figure()
        figure.update_layout(go.Layout(margin = dict(l = 20, r = 20, t = 20, b = 20), template = 'simple_white', showlegend = False,
                                       xaxis = {'showgrid': True, 'showline': True, 'mirror': True, 'titlefont': {'size': 20}, 'tickfont': {'size': 16},
                                                'tickangle': -90, 'title': self.col_x},
                                       yaxis = {'showgrid': True, 'showline': True, 'mirror': True, 'titlefont': {'size': 20}, 'tickfont': {'size': 16},
                                                'tickformat': f'.{n_digits}f', 'title': label_y},
                                       font = {'size': 28}, autosize = False, width = 900, height = 500, hovermode = 'closest'))
        if dashboard.col_x == 'Day of month':
            figure.update_layout(go.Layout(xaxis = {'tickmode': 'array', 'tickvals': [f'2000-01-{str(i).zfill(2)} 00:00:00' for i in np.arange(1, 32, 2)],
                                                    'ticktext': [f'{i}' for i in np.arange(1, 32, 2)],'showgrid': True, 'showline': True, 'mirror': True,
                                                    'titlefont': {'size': 20}, 'tickfont': {'size': 16}, 'tickangle': 0}))
        #
        if dashboard.plot_type == 'Lines':
            if dashboard.col_color is None:
                figure.add_trace(go.Scatter(x = df[dashboard.col_x], y = df['Metric'], mode = 'lines'))
            else:
                for breakdown in df[dashboard.col_color].unique():
                    if dashboard.col_x != 'Day of month':
                        figure.add_trace(go.Scatter(x = df.loc[df[dashboard.col_color] == breakdown, dashboard.col_x],
                                                    y = df.loc[df[dashboard.col_color] == breakdown, 'Metric'],
                                                    name = f'{breakdown}', mode = 'lines'))
                    else:
                        figure.add_trace(go.Scatter(x = df.loc[df[dashboard.col_color] == breakdown, dashboard.col_x],
                                                    y = df.loc[df[dashboard.col_color] == breakdown, 'Metric'],
                                                    name = f'{breakdown}', mode = 'lines', hovertemplate = 'Day %{x|%d}: %{x|%H:%M:%S}'))
        elif dashboard.plot_type == 'Bars':
            if dashboard.col_color is None:
                figure.add_trace(go.Bar(x = df[dashboard.col_x], y = df['Metric'], width = 0.5, offset = -0.5))
            else:
                for breakdown in df[dashboard.col_color].unique():
                    if dashboard.col_x != 'Day of month':
                        figure.add_trace(go.Bar(x = df.loc[df[dashboard.col_color] == breakdown, dashboard.col_x],
                                                    y = df.loc[df[dashboard.col_color] == breakdown, 'Metric'],
                                                    name = f'{breakdown}', width = 0.5, offset = -0.5))
                    else:
                        figure.add_trace(go.Bar(x = df.loc[df[dashboard.col_color] == breakdown, dashboard.col_x],
                                                    y = df.loc[df[dashboard.col_color] == breakdown, 'Metric'],
                                                    name = f'{breakdown}', width = 0.5, offset = -0.5, hovertemplate = 'Day %{x|%d}: %{x|%H:%M:%S}'))
        self.df = df
        return figure

    def _plot_time_1_metric(self, figure):
        '''
        Function to plot vertical lines corresponding to end of session and settlement (when available), together with rectangles indicating Asian,
        European and American trading sessions. It has effect only when 'Time' is the variable to group on.

        Args:
            figure: Figure built by the function `_plot_1_metric` (or `_plot_2_metrics`).

        Returns: None.
        '''
        df = self.df.copy()
        # add vertical lines i `Time` is a column of `df`
        if 'Time' in df.columns:
            if ((self._plot_trading_sessions == 'Yes') and (dashboard.filter_time[0] == dashboard.sess_start) and
                (dashboard.filter_time[1] == dashboard.sess_end)):
                figure = self._plot_rect_session_1_metric(figure)
            if (self._plot_rth == 'Yes') and (dashboard.filter_time[0] == dashboard.sess_start) and (dashboard.filter_time[1] == dashboard.sess_end):
                figure = self._plot_rect_rth_1_metric(figure)
            #
            if (dashboard.filter_time[0] == dashboard.sess_start) and (dashboard.filter_time[1] == dashboard.sess_end):
                df_temp = pd.DataFrame({'sess_end': [self.sess_end], 'label': ['End of session'],
                                        'y': [df['Metric'].max() - 0.12*(df['Metric'].max() - df['Metric'].min())]})
                figure.add_vline(x = self.sess_end, line_width = 2, line_dash = 'dash', line_color = 'cyan')
                figure.add_annotation(x = self.sess_end, y = df_temp['y'].values[0], text = 'End session', font = {'size': 14, 'color': 'cyan'},
                                    textangle = -90, xshift = 20)
            if ((self.instrument in self.dict_settlement_hour.keys()) and (dashboard.filter_time[0] == dashboard.sess_start) and
                (dashboard.filter_time[1] == dashboard.sess_end)):
                df_temp = pd.DataFrame({'settlement': [self.dict_settlement_hour[self.instrument]], 'label': ['Settlement time'],
                                        'y': [df['Metric'].max() - 0.15*(df['Metric'].max() - df['Metric'].min())]})
                figure.add_vline(x = self.dict_settlement_hour[self.instrument], line_width = 2, line_dash = 'dash', line_color = 'orange')
                figure.add_annotation(x = self.dict_settlement_hour[self.instrument], y = df_temp['y'].values[0], text = 'Settlement',
                                      font = {'size': 14, 'color': 'orange'}, textangle = -90, xshift = 0)
        return figure
    
    def _plot_rect_session_1_metric(self, figure):
        '''
        Function to plot rectangles indicating Asian, European and American trading sessions. It is called by the function `_plot_time_1_metric`.

        Args:
            figure: Figure built by the function `_plot`.

        Returns: None.
        '''
        df = self.df.copy()
        #
        start_sess = self.dict_sess[dashboard.instrument][0]
        if start_sess == '17:00:00':
            figure.add_vrect(x0 = df.loc[np.where(df['Time'] >= pd.to_datetime('17:00:00').time())[0].min(), 'Time'], x1 = '00:00:00', fillcolor = 'yellow',
                             opacity = 0.15, line_width = 0)
            figure.add_vrect(x0 = '00:00:00', x1 = '01:00:00', fillcolor = 'yellow', opacity = 0.15, line_width = 0)
            figure.add_annotation(x = '21:00:00', y = df['Metric'].min()*1.1, text = 'Asia', font = {'size': 18, 'color': 'white'}, yanchor = 'top')
            figure.add_vrect(x0 = '01:00:00', x1 = '08:00:00', fillcolor = 'red', opacity = 0.15, line_width = 0)
            figure.add_annotation(x = '04:30:00', y = df['Metric'].min()*1.1, text = 'Europe', font = {'size': 18, 'color': 'white'}, yanchor = 'top')
            figure.add_vrect(x0 = '08:00:00', x1 = '16:00:00', fillcolor = 'blue', opacity = 0.15, line_width = 0)
            figure.add_annotation(x = '12:00:00', y = df['Metric'].min()*1.1, text = 'US', font = {'size': 18, 'color': 'white'}, yanchor = 'top')
        if start_sess == '18:00:00':
            figure.add_vrect(x0 = df.loc[np.where(df['Time'] >= pd.to_datetime('18:00:00').time())[0].min(), 'Time'], x1 = '00:00:00', fillcolor = 'yellow',
                             opacity = 0.15, line_width = 0)
            figure.add_vrect(x0 = '00:00:00', x1 = '02:00:00', fillcolor = 'yellow', opacity = 0.15, line_width = 0)
            figure.add_annotation(x = '22:00:00', y = df['Metric'].min()*1.1, text = 'Asia', font = {'size': 18, 'color': 'white'}, yanchor = 'top')
            figure.add_vrect(x0 = '02:00:00', x1 = '09:00:00', fillcolor = 'red', opacity = 0.15, line_width = 0)
            figure.add_annotation(x = '05:30:00', y = df['Metric'].min()*1.1, text = 'Europe', font = {'size': 18, 'color': 'white'}, yanchor = 'top')
            figure.add_vrect(x0 = '09:00:00', x1 = '17:00:00', fillcolor = 'blue', opacity = 0.15, line_width = 0)
            figure.add_annotation(x = '13:00:00', y = df['Metric'].min()*1.1, text = 'US', font = {'size': 18, 'color': 'white'}, yanchor = 'top')
        if dashboard.instrument == 'FDAX':
            figure.add_vrect(x0 = df.loc[max(np.where(df['Time'] >= pd.to_datetime('01:10:00').time())[0].min() - 1, 0), 'Time'],
                             x1 = df.loc[np.where(df['Time'] <= pd.to_datetime('08:00:00').time())[0].max(), 'Time'], fillcolor = 'yellow', opacity = 0.15,
                             line_width = 0)
            figure.add_annotation(x = '04:30:00', y = df['Metric'].min()*1.1, text = 'Asia', font = {'size': 18, 'color': 'white'}, yanchor = 'top')
            figure.add_vrect(x0 = '08:00:00', x1 = '15:00:00', fillcolor = 'red', opacity = 0.15, line_width = 0)
            figure.add_annotation(x = '11:30:00', y = df['Metric'].min()*1.1, text = 'Europe', font = {'size': 18, 'color': 'white'}, yanchor = 'top')
            figure.add_vrect(x0 = '15:00:00', x1 = '22:00:00', fillcolor = 'blue', opacity = 0.15, line_width = 0)
            figure.add_annotation(x = '19:00:00', y = df['Metric'].min()*1.1, text = 'US', font = {'size': 18, 'color': 'white'}, yanchor = 'top')
        return figure
    
    def _plot_rect_rth_1_metric(self, figure):
        '''
        Function to plot a rectangle indicating regular trading hour. It is called by the function `_plot_time_1_metric`.

        Args:
            figure: Figure built by the function `_plot`.

        Returns: None.
        '''
        df = self.df.copy()
        first_time = self.dict_rth[self.instrument][0]
        second_time = self.dict_rth[self.instrument][1]
        #
        first_time_conv = datetime.datetime.strptime(first_time, '%H:%M:%S')
        second_time_conv = datetime.datetime.strptime(second_time, '%H:%M:%S')
        #
        figure.add_vrect(x0 = first_time, x1 = second_time, fillcolor = 'orange', opacity = 0.15, line_width = 0)
        figure.add_annotation(x = (first_time_conv + (second_time_conv - first_time_conv)/2).strftime('%H:%M:%S'), y = df['Metric'].min()*1.1,
                              text = 'Regular trading hours', font = {'size': 17, 'color': 'white'}, yanchor = 'top')
        return figure
    
    def _plot_tops_bottoms(self):
        '''
        Function to plot the highest and lowest values of the chosen metric.

        Args: None.

        Returns: None.
        '''
        df = self.df.copy()
        df = df.sort_values(by = 'Metric', ascending = False).reset_index(drop = True)
        #
        color_top, color_bottom = 'blue', 'red'
        df_top = df.iloc[:15]
        df_bottom = df.iloc[-15:]
        if self.metric in ['Close', 'Body', 'Range', 'Num highs', 'Num lows', 'Num highs or lows', 'Volume']:
            df_bottom = df.iloc[15:30]
            color_bottom = 'blue'
        #
        df_top = df_top.iloc[::-1]
        df_bottom = df_bottom.iloc[::-1]
        # number of digits to use
        n_digits = 0
        if np.diff(df['Metric'].drop_duplicates().sort_values().values[:2])[0] > 0:
            n_digits = len(decimal.Decimal(round(1/np.diff(df['Metric'].drop_duplicates().sort_values().values[:2])[0])).as_tuple().digits)
        #
        label_x = self.metric
        if label_x in ['Close', 'Delta close', 'Body', 'Range', 'Open-high', 'Open-low']:
            label_x += f' [{self.unit}]'
        #
        figure = go.Figure()
        figure.update_layout(go.Layout(margin = dict(l = 20, r = 20, t = 20, b = 20), template = 'simple_white', showlegend = False,
                                       xaxis = {'showgrid': True, 'showline': True, 'mirror': True, 'titlefont': {'size': 20}, 'tickfont': {'size': 12},
                                                'tickformat': f'.{n_digits}f', 'title': label_x},
                                       yaxis = {'showgrid': True, 'showline': True, 'mirror': True, 'titlefont': {'size': 20}, 'tickfont': {'size': 12},
                                                'title': self.col_x},
                                       font = {'size': 28}, autosize = False, width = 900, height = 550))
        #
        figure.add_trace(go.Bar(x = df_bottom['Metric'], y = df_bottom[self.col_x], marker_color = color_bottom, orientation = 'h'))
        figure.add_trace(go.Bar(x = df_top['Metric'], y = df_top[self.col_x], marker_color = color_top, orientation = 'h'))
        #
        return figure

    def _plot_2_metrics(self):
        '''
        Function to plot the main chart.

        Args: None.

        Returns: None.
        '''
        df = self.df.copy()
        # number of digits to use
        if np.diff(df['Metric_1'].drop_duplicates().sort_values().values[:2])[0] > 0:
            n_digits_1 = len(decimal.Decimal(round(1/np.diff(df['Metric_1'].drop_duplicates().sort_values().values[:2])[0])).as_tuple().digits)
        if np.diff(df['Metric_2'].drop_duplicates().sort_values().values[:2])[0] > 0:
            n_digits_2 = len(decimal.Decimal(round(1/np.diff(df['Metric_2'].drop_duplicates().sort_values().values[:2])[0])).as_tuple().digits)
        # add units to y axis labels
        label_y_1 = self.metric[0]
        label_y_2 = self.metric[1]
        if label_y_1 in ['Close', 'Delta close', 'Body', 'Range', 'Open-high', 'Open-low']:
            label_y_1 += f' [{self.unit}]'
        if label_y_2 in ['Close', 'Delta close', 'Body', 'Range', 'Open-high', 'Open-low']:
            label_y_2 += f' [{self.unit}]'
        # shift time back to original values: this way, the first row corresponds to session start
        if dashboard.col_x == 'Time':
            df['Time'] = (pd.to_datetime('2000-01-01 ' + df['Time']) +
                          pd.Timedelta(eval(self.sess_start.split(':')[0].lstrip('0')), unit = 'h')).dt.time
        #
        figure = go.Figure()
        figure = make_subplots(rows = 2, cols = 1, shared_xaxes = True)
        figure.update_layout(go.Layout(margin = dict(l = 20, r = 20, t = 20, b = 20), template = 'simple_white', showlegend = False,
                                       xaxis1 = {'showgrid': True, 'showline': True, 'mirror': True, 'titlefont': {'size': 20}, 'tickfont': {'size': 16},
                                                 'tickangle': -90, 'title': None},
                                       yaxis1 = {'showgrid': True, 'showline': True, 'mirror': True, 'titlefont': {'size': 20}, 'tickfont': {'size': 16},
                                                 'tickformat': f'.{n_digits_1}f', 'title': label_y_1},
                                       xaxis2 = {'showgrid': True, 'showline': True, 'mirror': True, 'titlefont': {'size': 20}, 'tickfont': {'size': 16},
                                                 'tickangle': -90, 'title': self.col_x},
                                       yaxis2 = {'showgrid': True, 'showline': True, 'mirror': True, 'titlefont': {'size': 20}, 'tickfont': {'size': 16},
                                                 'tickformat': f'.{n_digits_2}f', 'title': label_y_2},
                                       font = {'size': 28}, autosize = False, width = 900, height = 500))
        #
        if dashboard.plot_type == 'Lines':
            if dashboard.col_color is None:
                figure.add_trace(go.Scatter(x = df[dashboard.col_x], y = df['Metric_1'], mode = 'lines'), row = 1, col = 1)
                figure.add_trace(go.Scatter(x = df[dashboard.col_x], y = df['Metric_2'], mode = 'lines'), row = 2, col = 1)
            else:
                for breakdown in df[dashboard.col_color].unique():
                    figure.add_trace(go.Scatter(x = df.loc[df[dashboard.col_color] == breakdown, dashboard.col_x],
                                                y = df.loc[df[dashboard.col_color] == breakdown, 'Metric_1'],
                                                name = f'{breakdown}', mode = 'lines'), row = 1, col = 1)
                    figure.add_trace(go.Scatter(x = df.loc[df[dashboard.col_color] == breakdown, dashboard.col_x],
                                                y = df.loc[df[dashboard.col_color] == breakdown, 'Metric_2'],
                                                name = f'{breakdown}', mode = 'lines'), row = 2, col = 1)
        elif dashboard.plot_type == 'Bars':
            if dashboard.col_color is None:
                figure.add_trace(go.Bar(x = df[dashboard.col_x], y = df['Metric_1'], width = 0.5, offset = -0.5), row = 1, col = 1)
                figure.add_trace(go.Bar(x = df[dashboard.col_x], y = df['Metric_2'], width = 0.5, offset = -0.5), row = 2, col = 1)
            else:
                for breakdown in df[dashboard.col_color].unique():
                    figure.add_trace(go.Bar(x = df.loc[df[dashboard.col_color] == breakdown, dashboard.col_x],
                                                y = df.loc[df[dashboard.col_color] == breakdown, 'Metric_1'],
                                                name = f'{breakdown}', width = 0.5, offset = -0.5), row = 1, col = 1)
                    figure.add_trace(go.Bar(x = df.loc[df[dashboard.col_color] == breakdown, dashboard.col_x],
                                                y = df.loc[df[dashboard.col_color] == breakdown, 'Metric_2'],
                                                name = f'{breakdown}', width = 0.5, offset = -0.5), row = 2, col = 1)
        self.df = df
        return figure

    def _plot_time_2_metrics(self, figure):
        '''
        Function to plot vertical lines corresponding to end of session and settlement (when available), together with rectangles indicating Asian,
        European and American trading sessions. It has effect only when 'Time' is the variable to group on.

        Args:
            figure: Figure built by the function `_plot_1_metric` (or `_plot_2_metrics`).

        Returns: None.
        '''
        df = self.df.copy()
        # add vertical lines i `Time` is a column of `df`
        if 'Time' in df.columns:
            if (self._plot_trading_sessions == 'Yes') and (dashboard.filter_time[0] == dashboard.sess_start) and (dashboard.filter_time[1] == dashboard.sess_end):
                figure = self._plot_rect_session_2_metrics(figure)
            if (self._plot_rth == 'Yes') and (dashboard.filter_time[0] == dashboard.sess_start) and (dashboard.filter_time[1] == dashboard.sess_end):
                figure = self._plot_rect_rth_2_metrics(figure)
            #
            if (dashboard.filter_time[0] == dashboard.sess_start) and (dashboard.filter_time[1] == dashboard.sess_end):
                df_temp = pd.DataFrame({'sess_end': [self.sess_end], 'label': ['End of session'],
                                        'y': [df['Metric_1'].max() - 0.12*(df['Metric_1'].max() - df['Metric_1'].min())]})
                figure.add_vline(x = self.sess_end, line_width = 2, line_dash = 'dash', line_color = 'cyan', row = 1, col = 1)
                figure.add_annotation(x = self.sess_end, y = df_temp['y'].values[0], text = 'End session', font = {'size': 14, 'color': 'cyan'},
                                    textangle = -90, xshift = 20, row = 1, col = 1)
                df_temp = pd.DataFrame({'sess_end': [self.sess_end], 'label': ['End of session'],
                                        'y': [df['Metric_2'].max() - 0.12*(df['Metric_2'].max() - df['Metric_2'].min())]})
                figure.add_vline(x = self.sess_end, line_width = 2, line_dash = 'dash', line_color = 'cyan', row = 2, col = 1)
                figure.add_annotation(x = self.sess_end, y = df_temp['y'].values[0], text = 'End session', font = {'size': 14, 'color': 'cyan'},
                                    textangle = -90, xshift = 20, row = 2, col = 1)
            if ((self.instrument in self.dict_settlement_hour.keys()) and (dashboard.filter_time[0] == dashboard.sess_start) and
                (dashboard.filter_time[1] == dashboard.sess_end)):
                df_temp = pd.DataFrame({'settlement': [self.dict_settlement_hour[self.instrument]], 'label': ['Settlement time'],
                                        'y': [df['Metric_1'].max() - 0.15*(df['Metric_1'].max() - df['Metric_1'].min())]})
                figure.add_vline(x = self.dict_settlement_hour[self.instrument], line_width = 2, line_dash = 'dash', line_color = 'orange', row = 1, col = 1)
                figure.add_annotation(x = self.dict_settlement_hour[self.instrument], y = df_temp['y'].values[0], text = 'Settlement',
                                      font = {'size': 14, 'color': 'orange'}, textangle = -90, xshift = 0, row = 1, col = 1)
                df_temp = pd.DataFrame({'settlement': [self.dict_settlement_hour[self.instrument]], 'label': ['Settlement time'],
                                        'y': [df['Metric_2'].max() - 0.15*(df['Metric_2'].max() - df['Metric_2'].min())]})
                figure.add_vline(x = self.dict_settlement_hour[self.instrument], line_width = 2, line_dash = 'dash', line_color = 'orange', row = 2, col = 1)
                figure.add_annotation(x = self.dict_settlement_hour[self.instrument], y = df_temp['y'].values[0], text = 'Settlement',
                                      font = {'size': 14, 'color': 'orange'}, textangle = -90, xshift = 0, row = 2, col = 1)
        return figure
    
    def _plot_rect_session_2_metrics(self, figure):
        '''
        Function to plot rectangles indicating Asian, European and American trading sessions. It is called by the function `_plot_time_2_metrics`.

        Args:
            figure: Figure built by the function `_plot`.

        Returns: None.
        '''
        df = self.df.copy()
        #
        start_sess = self.dict_sess[dashboard.instrument][0]
        if start_sess == '17:00:00':
            figure.add_vrect(x0 = df.loc[np.where(df['Time'] >= pd.to_datetime('17:00:00').time())[0].min(), 'Time'], x1 = '00:00:00', fillcolor = 'yellow',
                             opacity = 0.15, line_width = 0, row = 1, col = 1)
            figure.add_vrect(x0 = '00:00:00', x1 = '01:00:00', fillcolor = 'yellow', opacity = 0.15, line_width = 0, row = 1, col = 1)
            figure.add_annotation(x = '21:00:00', y = df['Metric_1'].min()*1.1, text = 'Asia', font = {'size': 18, 'color': 'white'}, yanchor = 'top',
                                  row = 1, col = 1)
            figure.add_vrect(x0 = '01:00:00', x1 = '08:00:00', fillcolor = 'red', opacity = 0.15, line_width = 0, row = 1, col = 1)
            figure.add_annotation(x = '04:30:00', y = df['Metric_1'].min()*1.1, text = 'Europe', font = {'size': 18, 'color': 'white'}, yanchor = 'top',
                                  row = 1, col = 1)
            figure.add_vrect(x0 = '08:00:00', x1 = '16:00:00', fillcolor = 'blue', opacity = 0.15, line_width = 0, row = 1, col = 1)
            figure.add_annotation(x = '12:00:00', y = df['Metric_1'].min()*1.1, text = 'US', font = {'size': 18, 'color': 'white'}, yanchor = 'top',
                                  row = 1, col = 1)
            #
            figure.add_vrect(x0 = df.loc[np.where(df['Time'] >= pd.to_datetime('17:00:00').time())[0].min(), 'Time'], x1 = '00:00:00', fillcolor = 'yellow',
                             opacity = 0.15, line_width = 0, row = 2, col = 1)
            figure.add_vrect(x0 = '00:00:00', x1 = '01:00:00', fillcolor = 'yellow', opacity = 0.15, line_width = 0, row = 2, col = 1)
            figure.add_annotation(x = '21:00:00', y = df['Metric_2'].min()*1.1, text = 'Asia', font = {'size': 18, 'color': 'white'}, yanchor = 'top',
                                  row = 2, col = 1)
            figure.add_vrect(x0 = '01:00:00', x1 = '08:00:00', fillcolor = 'red', opacity = 0.15, line_width = 0, row = 2, col = 1)
            figure.add_annotation(x = '04:30:00', y = df['Metric_2'].min()*1.1, text = 'Europe', font = {'size': 18, 'color': 'white'}, yanchor = 'top',
                                  row = 2, col = 1)
            figure.add_vrect(x0 = '08:00:00', x1 = '16:00:00', fillcolor = 'blue', opacity = 0.15, line_width = 0, row = 2, col = 1)
            figure.add_annotation(x = '12:00:00', y = df['Metric_2'].min()*1.1, text = 'US', font = {'size': 18, 'color': 'white'}, yanchor = 'top',
                                  row = 2, col = 1)
        if start_sess == '18:00:00':
            figure.add_vrect(x0 = df.loc[np.where(df['Time'] >= pd.to_datetime('18:00:00').time())[0].min(), 'Time'], x1 = '00:00:00', fillcolor = 'yellow',
                             opacity = 0.15, line_width = 0, row = 1, col = 1)
            figure.add_vrect(x0 = '00:00:00', x1 = '02:00:00', fillcolor = 'yellow', opacity = 0.15, line_width = 0, row = 1, col = 1)
            figure.add_annotation(x = '22:00:00', y = df['Metric_1'].min()*1.1, text = 'Asia', font = {'size': 18, 'color': 'white'}, yanchor = 'top',
                                  row = 1, col = 1)
            figure.add_vrect(x0 = '02:00:00', x1 = '09:00:00', fillcolor = 'red', opacity = 0.15, line_width = 0, row = 1, col = 1)
            figure.add_annotation(x = '05:30:00', y = df['Metric_1'].min()*1.1, text = 'Europe', font = {'size': 18, 'color': 'white'}, yanchor = 'top',
                                  row = 1, col = 1)
            figure.add_vrect(x0 = '09:00:00', x1 = '17:00:00', fillcolor = 'blue', opacity = 0.15, line_width = 0, row = 1, col = 1)
            figure.add_annotation(x = '13:00:00', y = df['Metric_1'].min()*1.1, text = 'US', font = {'size': 18, 'color': 'white'}, yanchor = 'top',
                                  row = 1, col = 1)
            #
            figure.add_vrect(x0 = df.loc[np.where(df['Time'] >= pd.to_datetime('18:00:00').time())[0].min(), 'Time'], x1 = '00:00:00', fillcolor = 'yellow',
                             opacity = 0.15, line_width = 0, row = 2, col = 1)
            figure.add_vrect(x0 = '00:00:00', x1 = '02:00:00', fillcolor = 'yellow', opacity = 0.15, line_width = 0, row = 2, col = 1)
            figure.add_annotation(x = '22:00:00', y = df['Metric_2'].min()*1.1, text = 'Asia', font = {'size': 18, 'color': 'white'}, yanchor = 'top',
                                  row = 2, col = 1)
            figure.add_vrect(x0 = '02:00:00', x1 = '09:00:00', fillcolor = 'red', opacity = 0.15, line_width = 0, row = 2, col = 1)
            figure.add_annotation(x = '05:30:00', y = df['Metric_2'].min()*1.1, text = 'Europe', font = {'size': 18, 'color': 'white'}, yanchor = 'top',
                                  row = 2, col = 1)
            figure.add_vrect(x0 = '09:00:00', x1 = '17:00:00', fillcolor = 'blue', opacity = 0.15, line_width = 0, row = 2, col = 1)
            figure.add_annotation(x = '13:00:00', y = df['Metric_2'].min()*1.1, text = 'US', font = {'size': 18, 'color': 'white'}, yanchor = 'top',
                                  row = 2, col = 1)
        if dashboard.instrument == 'FDAX':
            figure.add_vrect(x0 = df.loc[max(np.where(df['Time'] >= pd.to_datetime('01:10:00').time())[0].min() - 1, 0), 'Time'],
                             x1 = df.loc[np.where(df['Time'] <= pd.to_datetime('08:00:00').time())[0].max(), 'Time'], fillcolor = 'yellow', opacity = 0.15,
                             line_width = 0, row = 1, col = 1)
            figure.add_annotation(x = '04:30:00', y = df['Metric_1'].min()*1.1, text = 'Asia', font = {'size': 18, 'color': 'white'}, yanchor = 'top',
                                  row = 1, col = 1)
            figure.add_vrect(x0 = '08:00:00', x1 = '15:00:00', fillcolor = 'red', opacity = 0.15, line_width = 0, row = 1, col = 1)
            figure.add_annotation(x = '11:30:00', y = df['Metric_1'].min()*1.1, text = 'Europe', font = {'size': 18, 'color': 'white'}, yanchor = 'top',
                                  row = 1, col = 1)
            figure.add_vrect(x0 = '15:00:00', x1 = '22:00:00', fillcolor = 'blue', opacity = 0.15, line_width = 0, row = 1, col = 1)
            figure.add_annotation(x = '19:00:00', y = df['Metric_1'].min()*1.1, text = 'US', font = {'size': 18, 'color': 'white'}, yanchor = 'top',
                                  row = 1, col = 1)
            #
            figure.add_vrect(x0 = df.loc[max(np.where(df['Time'] >= pd.to_datetime('01:10:00').time())[0].min() - 1, 0), 'Time'],
                             x1 = df.loc[np.where(df['Time'] <= pd.to_datetime('08:00:00').time())[0].max(), 'Time'], fillcolor = 'yellow', opacity = 0.15,
                             line_width = 0, row = 2, col = 1)
            figure.add_annotation(x = '04:30:00', y = df['Metric_2'].min()*1.1, text = 'Asia', font = {'size': 18, 'color': 'white'}, yanchor = 'top',
                                  row = 2, col = 1)
            figure.add_vrect(x0 = '08:00:00', x1 = '15:00:00', fillcolor = 'red', opacity = 0.15, line_width = 0, row = 2, col = 1)
            figure.add_annotation(x = '11:30:00', y = df['Metric_2'].min()*1.1, text = 'Europe', font = {'size': 18, 'color': 'white'}, yanchor = 'top',
                                  row = 2, col = 1)
            figure.add_vrect(x0 = '15:00:00', x1 = '22:00:00', fillcolor = 'blue', opacity = 0.15, line_width = 0, row = 2, col = 1)
            figure.add_annotation(x = '19:00:00', y = df['Metric_2'].min()*1.1, text = 'US', font = {'size': 18, 'color': 'white'}, yanchor = 'top',
                                  row = 2, col = 1)
        return figure
    
    def _plot_rect_rth_2_metrics(self, figure):
        '''
        Function to plot a rectangle indicating regular trading hour. It is called by the function `_plot_time_2_metrics`.

        Args:
            figure: Figure built by the function `_plot`.

        Returns: None.
        '''
        df = self.df.copy()
        first_time = self.dict_rth[self.instrument][0]
        second_time = self.dict_rth[self.instrument][1]
        #
        first_time_conv = datetime.datetime.strptime(first_time, '%H:%M:%S')
        second_time_conv = datetime.datetime.strptime(second_time, '%H:%M:%S')
        #
        figure.add_vrect(x0 = first_time, x1 = second_time, fillcolor = 'orange', opacity = 0.15, line_width = 0, row = 1, col = 1)
        figure.add_annotation(x = (first_time_conv + (second_time_conv - first_time_conv)/2).strftime('%H:%M:%S'), y = df['Metric_1'].min()*1.1,
                              text = 'Regular trading hours', font = {'size': 17, 'color': 'white'}, yanchor = 'top', row = 1, col = 1)
        figure.add_vrect(x0 = first_time, x1 = second_time, fillcolor = 'orange', opacity = 0.15, line_width = 0, row = 2, col = 1)
        figure.add_annotation(x = (first_time_conv + (second_time_conv - first_time_conv)/2).strftime('%H:%M:%S'), y = df['Metric_2'].min()*1.1,
                              text = 'Regular trading hours', font = {'size': 17, 'color': 'white'}, yanchor = 'top', row = 2, col = 1)
        return figure

if __name__ == '__main__':
    # login
    if not check_password():
        st.stop()
    # page width
    st.set_page_config(layout = 'wide')
    #
    with st.form(key = 'Main run'):
        # define the filters
        dashboard = Dashboard(max_rows = 250000)
        #
        run = st.form_submit_button(label = 'Run')
    # run the dashboard
    if run == True:
        #
        dashboard._filter_dates()
        dashboard._filter_times()
        dashboard._filter_month()
        dashboard._filter_day_of_month()
        dashboard._filter_day_of_week()
        #
        dashboard._group_to_timeframe()
        #
        dashboard._compute_metric()
        dashboard._add_split_period()
        dashboard._group_data()
        #
        dashboard._adjust_timeframe()
        #
        dashboard._fix_missing_dates()
        #
        df = dashboard.df
        df.columns = df.columns.str.capitalize()
        dashboard.col_x = dashboard.col_x.capitalize()
        if dashboard.col_color is not None:
            dashboard.col_color = dashboard.col_color.capitalize()
        # plot
        if dashboard.n_metrics == 1:
            if dashboard.plot_tops_bottoms == 'No':
                figure = dashboard._plot_1_metric()
                figure = dashboard._plot_time_1_metric(figure)
            else:
                figure = dashboard._plot_tops_bottoms()
        else:
            figure = dashboard._plot_2_metrics()
            figure = dashboard._plot_time_2_metrics(figure)
        figure.update_layout(xaxis_rangeslider_visible = False)
        st.plotly_chart(figure)