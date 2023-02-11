#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Oct 24 19:49:20 2022

@author: michelev
"""

from bs4 import BeautifulSoup
import requests
import lxml
import re
import json
import urllib

import numpy as np
import pandas as pd

import os
from concurrent.futures import ThreadPoolExecutor

import datetime

import Scraper
#%%

class ScraperRT:
    
    # 8 main categories of news in RussiaToday: the English words are recognized also in the site in original language
    CATEGORIES = ['world', 'russia', 'ussr', 'business', 'sport', 'science', 'nopolitics', 'opinion']
    
    proxies = {'http': 'http://lum-customer-hl_33f39684-zone-zone1:9tzrgl2f2e55@zproxy.lum-superproxy.io:22225',
               'https': 'http://lum-customer-hl_33f39684-zone-zone1:9tzrgl2f2e55@zproxy.lum-superproxy.io:22225'}
    
    headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.5060.114 Safari/537.36"
    }
    
    # write the url used for scraping: usually we will use start_date = end_date while searching for each date separately
    def writeUrl(self, category, start_date, end_date, page):
        
        url = 'https://russian.rt.com/'
        url += 'search/?q=' + category 
        url += '&type=&category=&xcategory=&'
        url += 'df=' + start_date
        url += '&' + 'dt=' + end_date 
        url += 'u&pageSize=0'
        url += '&page=' + str(page)
        url += '&format='
        
        return url
    
    def findArticles(self, dataframe, url):
    
        # get response using proxies
        response = Scraper.getResponseProxies(url, self.headers, self.proxies)
        # filter source for articles
        soup = BeautifulSoup(response.content, 'lxml')
        articles = soup.find_all('li', class_="listing__column listing__column_all-new listing__column_js")
    
        # fill dataframe with articles
        for article in articles:
                    
            # find url of the article
            link = re.findall('(?<=href=").*(?=")', article.decode())[0]
            
            # split title, subtitle, date/time
            strings = np.array(list(map(str.strip, article.text.split('\n'))))
            strings = strings[np.nonzero(list(map(len,strings)))[0]]
            
            title = strings[0]
            subtitle = strings[1]      
            
            time = strings[2].split(', ')
            day = time[0]
            time = time[1]
            
            row = {'link': link, 'title': title, 'subtitle': subtitle, 'day': day, 'time': time}
            
            dataframe = pd.concat((dataframe, pd.DataFrame(row, index=[0])), ignore_index=True)
            dataframe = dataframe.drop_duplicates(subset=['link'])
            
        return dataframe

    def searchTitles(self, date, category):
        
        # create dataframe containing article generic info (link, title, subtitle, day/time)
        # of a specific category of news for a specific day
        dataframe = pd.DataFrame(columns=['link', 'title', 'subtitle', 'day', 'time'])
        # search a new page only if you found new articles 
        len_diff = 100
        previous_len = len(dataframe)
        # start from page 1
        page = 1
        while len_diff>0:
            # write url for category and date
            url = self.writeUrl(category, date, date, page)
            # fill the dataframe
            dataframe = self.findArticles(dataframe, url)
            len_diff = len(dataframe) - previous_len
            previous_len = len(dataframe)
            page += 1
        # the search returns part of the following day: filter the dataframe to return only the required day
        last_day = dataframe.iloc[-1]['day']
        dataframe = dataframe[dataframe['day']==last_day]
        print('Date: '  + date + ' --- Found ' + str(previous_len) + ' results.')
        
        return dataframe
            
    def saveDataFrameGeneric(self, date, category = 'russia'):
        
        # save articles in path
        path = '/Users/michelev/russia/articles/RT/' + date.strftime('%Y-%m-%d')
        
        if os.path.exists(path) == False:
            os.mkdir(path)
        
        file_path = path + '/' + date.strftime('%Y-%m-%d') + '_' + category + '.csv'
        if os.path.exists(file_path):
            print('File already exists.')
        else:
            dataframe = self.searchTitles(date.strftime('%d-%m-%Y'), category)
            dataframe.to_csv(file_path)
            
#%%

RT = ScraperRT()
dates = pd.date_range(start='2021-01-01', end='2021-12-31', inclusive = 'both')
dates = list(dates)

for category in RT.CATEGORIES:
    
    with ThreadPoolExecutor(max_workers=10) as executor:
            executor.map(lambda date: RT.saveDataFrameGeneric(date, category),    
            dates,
            timeout = 3600)


  # https://api.tlgrm.app/v3/channels/1387645188/feed?offset_id=0
   
   
   
   
   
   
   
   