#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Feb 10 22:52:59 2023

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

class ScraperLenta:
        
    # 8 main categories of news in Lenta: the English words are recognized also in the site in original language
    CATEGORIES = ['russia',
                  'world',
                  'ussr',
                  'economics',
                  'forces',
                  'science',
                  'sport',
                  'culture',
                  'media',
                  'style',
                  'travel',
                  'life',
                  'realty',
                  'wellness']
    
    proxies = {'http': 'http://lum-customer-hl_33f39684-zone-zone1:9tzrgl2f2e55@zproxy.lum-superproxy.io:22225',
               'https': 'http://lum-customer-hl_33f39684-zone-zone1:9tzrgl2f2e55@zproxy.lum-superproxy.io:22225'}
    
    headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.5060.114 Safari/537.36"
    }
    
    # write the url used for scraping: usually we will use start_date = end_date while searching for each date separately
    def writeUrl(self, category, date, page):
        
        url = 'https://lenta.ru/'
        url += 'rubrics/'
        url += category + '/'
        url += date
        url += '/page/' + str(page) + '/'
        
        return url
    
    def findArticles(self, dataframe, url):
    
        # get response using proxies
        response = Scraper.getResponseProxies(url, self.headers, self.proxies)
        # filter source for articles
        soup = BeautifulSoup(response.content, 'lxml')
        articles = soup.find_all('a', class_="card-full-news _archive")
    
        # fill dataframe with articles
        for article in articles:
                   
            link = re.findall('(?<=href=").*?(?=">)', article.decode())[0]
            
            pieces = article.findChildren()
            
            title = pieces[0].contents[0]
            time = pieces[2].contents[0].split(', ')
            
            day = time[0]
            time = time[1]
            
            row = {'link': link, 'title': title, 'day': day, 'time': time}
            
            dataframe = pd.concat((dataframe, pd.DataFrame(row, index=[0])), ignore_index=True)
            dataframe = dataframe.drop_duplicates(subset=['link'])
            
        return dataframe
    
    def searchTitles(self, date, category):
        
        # create dataframe containing article generic info (link, title, subtitle, day/time)
        # of a specific category of news for a specific day
        dataframe = pd.DataFrame(columns=['link', 'title', 'day', 'time'])
        # search a new page only if you found new articles 
        len_diff = 100
        previous_len = len(dataframe)
        # start from page 1
        page = 1
        while len_diff>0:
            # write url for category and date
            url = self.writeUrl(category, date, page)
            # fill the dataframe
            dataframe = self.findArticles(dataframe, url)
            len_diff = len(dataframe) - previous_len
            previous_len = len(dataframe)
            page += 1
        # the search returns part of the following day: filter the dataframe to return only the required day
        #last_day = dataframe.iloc[-1]['day']
        #dataframe = dataframe[dataframe['day']==last_day]
        print('Date: '  + date + ' --- Found ' + str(previous_len) + ' results.')
        
        return dataframe
            
    def saveDataFrameGeneric(self, date, category = 'russia'):
        
        # save articles in path
        path = '/Users/michelev/russia/articles/Lenta/' + date.strftime('%Y-%m-%d')
        
        if os.path.exists(path) == False:
            os.mkdir(path)
        
        file_path = path + '/' + date.strftime('%Y-%m-%d') + '_' + category + '.csv'
        if os.path.exists(file_path):
            print('File already exists.')
        else:
            dataframe = self.searchTitles(date.strftime('%Y/%m/%d'), category)
            dataframe.to_csv(file_path)
            
#%%

Lenta = ScraperLenta()
dates = pd.date_range(start='2020-01-01', end='2021-12-31', inclusive = 'both')
dates = list(dates)

for category in Lenta.CATEGORIES:
    with ThreadPoolExecutor(max_workers=10) as executor:
            executor.map(lambda date: Lenta.saveDataFrameGeneric(date, category),    
            dates,
            timeout = 3600)


  # https://api.tlgrm.app/v3/channels/1387645188/feed?offset_id=0
   
   
   