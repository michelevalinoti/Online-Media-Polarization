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

class ScraperPravda:
    
    proxies = {'http': 'http://lum-customer-hl_33f39684-zone-zone1:9tzrgl2f2e55@zproxy.lum-superproxy.io:22225',
               'https': 'http://lum-customer-hl_33f39684-zone-zone1:9tzrgl2f2e55@zproxy.lum-superproxy.io:22225'}
    
    headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.5060.114 Safari/537.36"
    }
    
    def writeUrl(self, date):
        
        url = 'https://www.pravda.ru/'
        url += 'archive/' 
        url += date + '/'
        
        return url
    
    def findArticles(self, dataframe, url):
    
        # get response using proxies
        response = Scraper.getResponseProxies(url, self.headers, self.proxies)
        # filter source for articles
        soup = BeautifulSoup(response.content, 'lxml')
        articles = soup.find_all('div', class_='article')
    
        # fill dataframe with articles
        for article in articles:
                    
            # find url of the article
            link = re.findall('(?<=href=").*(?="/)', article.decode())[0]
            
            title = article.find('a').text.strip()
            
            try:
                day = re.findall('(?<=datetime=").*(?=T)', article.decode())
                time = article.find('time').text.strip()
            except:
                day = None
                time = None
                
            row = {'link': link, 'title': title, 'day': day, 'time': time}
            
            dataframe = pd.concat((dataframe, pd.DataFrame(row, index=[0])), ignore_index=True)
            dataframe = dataframe.drop_duplicates(subset=['link'])
            
        return dataframe

    def searchTitles(self, date):
        
        # create dataframe containing article generic info (link, title, day/time)
        # of a specific category of news for a specific day
        dataframe = pd.DataFrame(columns=['link', 'title', 'day', 'time'])
        
        url = self.writeUrl(date)
        # fill the dataframe
        dataframe = self.findArticles(dataframe, url)
        print('Date: '  + date + ' --- Found ' + str(len(dataframe)) + ' results.')
        
        return dataframe
            
    def saveDataFrameGeneric(self, date):
        
        # save articles in path
        path = '/Users/michelev/russia/articles/Pravda/' + date.strftime('%Y-%m-%d')
        
        if os.path.exists(path) == False:
            os.mkdir(path)
        
        file_path = path + '/' + date.strftime('%Y-%m-%d') + '.csv'
        if os.path.exists(file_path):
            print('File already exists.')
        else:
            dataframe = self.searchTitles(date.strftime('%Y-%m-%d'))
            dataframe.to_csv(file_path)
            
#%%

Pravda = ScraperPravda()
dates = pd.date_range(start='2021-01-01', end='2021-12-31', inclusive = 'both')
dates = list(dates)

with ThreadPoolExecutor(max_workers=10) as executor:
        executor.map(lambda date: Pravda.saveDataFrameGeneric(date),    
        dates,
        timeout = 3600)


  # https://api.tlgrm.app/v3/channels/1387645188/feed?offset_id=0
   
   
   
   
   
   
   
   