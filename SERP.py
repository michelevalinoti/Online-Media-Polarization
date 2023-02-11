#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Nov 26 17:47:01 2022

@author: michelev
"""
import sys
import ssl
import json

import Scraper

proxies = {'http': 'http://brd-customer-hl_33f39684-zone-novserp:zf5trt1u8njy@zproxy.lum-superproxy.io:22225',
           'https': 'http://brd-customer-hl_33f39684-zone-novserp:zf5trt1u8njy@zproxy.lum-superproxy.io:22225'}


opener = Scraper.getOpenerSERP(proxies)
#res = opener.open('https://www.google.com/search?q=website%3Ahttp%3A%2F%2Fwww.themoscowtimes.com%2F2022%2F11%2F24%2F&lum_json=1').read()

url = 'https://www.google.com/search?q=site%3Awww.yelp.com%2Fbiz%2F+new-york+%22CLOSED%22+Restaurants+OR+Food&gl=us&start=0&num=100'
res = opener.open(url + '&lum_json=1').read()
res = json.loads(res)