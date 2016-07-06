# -*- coding: utf-8 -*-
"""
Created on Tue Jul 05 17:29:13 2016

@author: pac
test for a web spider
"""
import urllib
import time
import traceback
import requests


#提取child（信用信息页）
def getChildLink(name):
    '''这一段基本拜银龙的代码'''
    now = int(time.time()*1000L)##获取当前时间的时间戳，获取时间的前13位##
    url = """http://www.creditchina.gov.cn/credit_info_search?t=%d"""%now
    data={
            'keyword':'%s'%name,
        'searchtype':'0',
        'objectType':'2',
        'areas':'',
        'creditType':'',
        'dataType':'1',
        'areaCode':'',
        'templateId':'',
        'exact':'0',
        'page':'1'
        }
    headers = {
                'Accept':'text/plain, */*; q=0.01',
            'Accept-Encoding':'gzip, deflate',
            'Accept-Language':'zh-CN,zh;q=0.8,en;q=0.6',
            'Connection':'keep-alive',
            'Content-Length':'191',
            'Content-Type':'application/x-www-form-urlencoded; charset=UTF-8',
            'Cookie':'Hm_lvt_0076fef7e919d8d7b24383dc8f1c852a=1467272309,1467273243,1467277124,1467595261; Hm_lpvt_0076fef7e919d8d7b24383dc8f1c852a=1467598746',
            'Host':'www.creditchina.gov.cn',
            'Origin':'http://www.creditchina.gov.cn',
            'Referer':'http://www.creditchina.gov.cn/search_all',
            'User-Agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/45.0.2454.93 Safari/537.36',
            'X-Requested-With':'XMLHttpRequest',
            }
    data = urllib.urlencode(data)
    req= requests.Session().post(url,headers=headers,data=data)
    html=req.text

    #eval 扁平化，把html ‘u"xxx"'文本扁平化成字典（去除外围string格式，不是是否是unicode）
    #childt头文件
    url="http://www.creditchina.gov.cn/credit_info_detail?"
    ch_par_list= eval(html.replace('null','None'))['result']['results'] #去掉string的u""或""去掉，保留内部各式
    listLink=[] 
    for  ch_par in ch_par_list:
        data={'objectType':ch_par['objectType'],'encryStr':ch_par['encryStr'].replace('\n','')}
        data=urllib.urlencode(data)
        listLink.append(url+data)
    return listLink
#=====================================================================================================================


#获取网页内容
def getHtml(url):
    req = urllib2.Request(url)
    response = urllib2.urlopen(req)             # 发起请求
    html = response.read()  
    return html 
#=====================================================================================================================


#获取数据到df中
def getHtmlData(html):
    from bs4 import BeautifulSoup
    soup =BeautifulSoup(html)
    
    
    #获取根所有根节点li有数据的列表
    soup_li=soup.find_all('li')
    strings=[]
    #List=[]
    for i in soup_li:
        #寻找带数据的li
        if i.attrs=={'class': ['oneline']}:
           # List.append(i)
            Li_list=[]
            for j in  i.strings:
                Li_list.append(j.encode('utf-8'))
            strings.append(Li_list)

            
    #将数据组成对式（tag，conteng）--大表格式
    import pandas as pd
    columns=['tag','content']
    df=pd.DataFrame(columns=columns) #注，df.loc可以为空数据框进行赋值
    for i in  range(0,len(strings)):
    
        #提取标签组
        temp=[]
        for j in  strings[i]:
            temp.append(j.replace('\n','').replace('：','').strip())      
            
        if len(temp)>2:
            temp.remove('')
            
        #解析为df   
        df.loc[i,'tag']=temp[0]
        if len(temp)==1:
            df.loc[i,'content']=None
        else:
            df.loc[i,'content']=temp[1]
            
    #将插入的类型数据分开,生成可直接插入的list,并列数合并合并'''
    #注：loc是闭区间，iloc是左闭右开'''
    pg_table=[]
    index= df[df.tag=='企业名称'].index.tolist()
    index.append(len(df))
    
    for  k in range(0,len(index)-1):
        #分组数据框
        temp=df.iloc[index[k]:index[k+1],:]
        temp.index=map(lambda x:x.decode('utf-8'),temp.tag.tolist()) #标题必须是utf-8，要改格式-而内容却可以随意--着实坑        
        tempdf=pd.DataFrame(temp.loc[:,'content']).T
        tempdf.index=range(len(tempdf))
        pg_table.append(tempdf)
    return  pg_table
#=====================================================================================================================    

    
    
#插入mongodb    
def mongoConfig(coll,df):
    from pymongo import * 
    uri = "mongodb://127.0.0.1:27017"
    Client= MongoClient(uri)
    db = Client.dm
    collection=db[coll]
    collection.insert_many(df.to_dict('records'))
    Client.close() #是否关闭链接
#=====================================================================================================================

