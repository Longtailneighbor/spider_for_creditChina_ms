# -*- coding: utf-8 -*-
"""
Created on Tue Jul 05 17:29:13 2016

@author: pac
mian work
"""
import pandas as pd
import random
import time

partner_data=pd.read_csv('lyu.csv')
pid = partner_data.partner_id.tolist()
pname = partner_data.partner_name.tolist()
ptype = partner_data.partner_type.tolist()
j=0
if __name__ == '__main__':
    
    #按照名字进行循环读取网页
    for i in range(j,len(pname)):
        print i #打印
        try:
            listLink=getChildLink(pname[i])
        except:
            print 'i need sleep'
            time.sleep(300)
            listLink=[]
            j=i
        time.sleep(random.uniform(3,5))  
        print listLink
        
        
    #展示数据查询的（待加入插入模块）
        if listLink!=[]:
            #get the data-内部处理
            for url in listLink:
                #一个url一个页面的内容
                time.sleep(random.uniform(0.1,1))
                html=getHtml(url)
                mg_tb=getHtmlData(html)
                lenth=map(lambda x:len(x.columns),mg_tb)
                for  df in mg_tb:
                    #一个df代表 四大之一的内容, base-- good 7,lost 15,bad 14
                    df['pid']=pid[0]
                    df['ptype']=ptype[0]
                    if len(df.columns)==13: #for base
                        #因为是自然顺序所以mg_db里不会乱序
                        dfb=df
                        print dfb
                    if len(df.columns)==9:  #for good
                        if 11 in lenth:
                            df['siret']=dfb[u'工商注册号']
                            dfgood=df
                            print dfgood #打印
                        else:
                            df['siret']=None
                            dfgood=df
                            print dfgood #打印
                    if len(df.columns)==17: #for lost
                        if 11 in lenth:
                            df['siret']=dfb[u'工商注册号']
                            dflost=df
                        else:
                            df['siret']=None
                            dflost=df
                    if len(df.columns)==16:  #for bad
                        if 11 in lenth:
                            df['siret']=dfb[u'工商注册号']
                            dfbad=df
                        else:
                            df['siret']=None
                            dfbad=df