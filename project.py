# -*- coding: utf-8 -*-
"""
Spyder 编辑器

这是一个临时脚本文件。
"""


import pandas as pd
import numpy as np
import os
path = os.getcwd()#获取当前路径
print(path)

df = pd.read_csv('.\coderepo\eth_2018_price.csv')
df.info()
