from bs4 import BeautifulSoup
import os
import pandas as pd
import requests
import datetime as dt
import numpy as np
import pandas_datareader as web

url = 'https://www.nasdaq.com/api/v1/news-headlines-fetcher/{}/{}/30'
tickers = ['ENTER TICKER HERE']
headers = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
    "Accept-Encoding": "gzip, deflate",
    "Accept-Language": "en-GB,en;q=0.9,en-US;q=0.8,ml;q=0.7",
    "Connection": "keep-alive",
    "Host": "www.nasdaq.com",
    "Referer": "http://www.nasdaq.com",
    "Upgrade-Insecure-Requests": "1",
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.119 Safari/537.36"
}



#If we have a watchlist, we will need to loop through each stock in 3 of the functions
def getNews():
    #will end up putting for loop to traverse through pages
    #will increment var by 30 to show 30 different items at a time
    #Case IDEA: break loop once len of items are less than 30...
    main_df = pd.DataFrame()
    page = 0
    list_len = 30
    # for i in range(0, 100):
    while list_len == 30:
        page_src = requests.get(url.format(tickers[0], str(page)), headers=headers, verify=False)
        soup = BeautifulSoup(page_src.text, 'html.parser')

        #get the columns we will need (date and headlines)
        headline_list = [headline.getText().replace('\n', '') for headline in
                            soup.find_all('p', class_='quote-news-headlines__item-title')]
        date_list = [date.getText() for date in soup.find_all('span', class_='quote-news-headlines__date')]
        #we need to get length of list, if changes then we are at the end, we can use either list above
        list_len = len(headline_list)
        #we need date to be datetime objects, use np.arrays
        datetime_obj_list = np.zeros(len(date_list), dtype='datetime64[s]')
        for i, date in enumerate(date_list):
            if 'hour' in date or 'hours' in date or 'minutes' in date or 'seconds' in date:
                datetime_obj_list[i] = dt.datetime.date(dt.datetime.now())
            elif 'day' in date or 'days' in date:
                datetime_obj_list[i] = from_day(date)
            #condition for actual dates in string format
            else:
                datetime_obj_list[i] = from_month(date)

        for_df = {'Date': datetime_obj_list, 'News': headline_list}
        temp_df = pd.DataFrame(data=for_df)
        if main_df.empty:
            main_df = temp_df
        else:
            main_df = main_df.append(temp_df)
        page += 30
    main_df = merge_news(main_df)
    main_df.to_csv('csv_files/{}_news.csv'.format(tickers[0]))

def from_day(date):
    days = [1,2,3,4,5,6,7]
    for num in days:
        if str(num) in date:
            return str(dt.datetime.date(dt.datetime.now()) - dt.timedelta(days=num))

def from_month(date):
    months = {'Jan': '1', 'Feb': '2', 'Mar': '3', 'Apr': '4', 'May': '5', 'Jun': '6',
              'Jul': '7', 'Aug': '8', 'Sep': '9', 'Oct': '10', 'Nov': '11', 'Dec': '12'}
    for month in months:
        if month in date:
            date = date.replace(month, months[month]).replace(',', '').replace(' ', '-')
            return dt.datetime.date(dt.datetime.strptime(date, '%m-%d-%Y'))
def merge_news(df):
    to_merge = lambda a: ". ".join(a)
    df = df.groupby(by='Date', as_index=False).agg({'News': to_merge}).reset_index()
    return df

#get stock data after news
def getData():
    #We need the same range of data as the news
    news_df = pd.read_csv('csv_files/{}_news.csv'.format(tickers[0]))
    start = news_df['Date'][0]
    end = news_df['Date'][len(news_df)-1]
    df = web.DataReader(tickers[0], 'yahoo', start, end)
    df['Pct Change'] = ((df['Adj Close'] - df['Open']) / df['Adj Close'] * 100)
    #we need to add values to end up predicting (+ or - change)
    df['Sign'] = np.nan
    for i, percent in enumerate(df['Pct Change']):
        if percent > 1:
            df.iloc[i, df.columns.get_loc('Sign')] = 1
        elif percent < -1:
            df.iloc[i, df.columns.get_loc('Sign')] = -1
        else:
            df.iloc[i, df.columns.get_loc('Sign')] = 0
    df.to_csv('csv_files/{}_data.csv'.format(tickers[0]))

def mergeData():
    news_df = pd.read_csv('csv_files/{}_news.csv'.format(tickers[0]))
    data_df = pd.read_csv('csv_files/{}_data.csv'.format(tickers[0]))
    merged_df = news_df.merge(data_df, how='inner', on='Date', left_index=True)
    merged_df.drop(columns=['index', 'Unnamed: 0'], inplace=True)
    merged_df.to_csv('csv_files/{}_merged.csv'.format(tickers[0]))


if __name__ == '__main__':
    getNews()
    getData()
    mergeData()
