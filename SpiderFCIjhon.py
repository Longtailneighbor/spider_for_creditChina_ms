# -*- coding: utf-8 -*-
"""
Created on Tue Jul 05 17:29:13 2016

@author: pac
test for a web spider
"""







#==============================================================================
# -*- coding: utf-8 -*-
"""
Created on Tue Jul 05 17:29:13 2016

@author: pac
test for a web spider
"""
#获取次级练级（信用信息页）
def getChildLink(name):
    import time
    import urllib2
    import urllib
    import requests
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


#获取网页内容
def getHtml(url):
    import urllib2
    import urllib
    import requests
    req = urllib2.Request(url)
    response = urllib2.urlopen(req,timeout=20)             # 发起请求,timeout会跳出循环
    html = response.read()  
    return html 


    

#插入函数--次级
def mongoConfig(coll,df):
    from pymongo import * 
    uri = "mongodb://127.0.0.1:27017"
    Client= MongoClient(uri)
    db = Client.dm
    collection=db[coll]
    collection.insert_many(df.to_dict('records'))
    Client.close() #是否关闭链接
#==============================================================================


    
def getHtmlData(html,order):    
    from bs4 import  BeautifulSoup
    import pandas as pd
    soup =BeautifulSoup(html)
    #抓取div
    #div = soup.findAll('div',attrs={'class','creditsearch-tagsinfo'}) #attars包含就行，而不是全要求
    div = soup.findAll('div',class_="creditsearch-tagsinfo")  # 上一种是包含，这一种也是直接指定条件
    #
    base = div[0]
    good = div[1]
    bad = div[2]
    lost = div[3]
    #获取baselist,并utf-8转码与去除空格，并去除冒号与空格
    base_list=map(lambda x,:[num.encode('utf-8').replace('\n','').replace('：','').strip() for num in x.strings],base.findAll('li')) 
    good_list=map(lambda x,:[num.encode('utf-8').replace('\n','').replace('：','').strip() for num in x.strings],good.findAll('li')) 
    bad_list=map(lambda x,:[num.encode('utf-8').replace('\n','').replace('：','').strip() for num in x.strings],bad.findAll('li')) 
    lost_list=map(lambda x,:[num.encode('utf-8').replace('\n','').replace('：','').strip() for num in x.strings],lost.findAll('li')) 

    '''数据处理'''    
    #去除数据中的空格-第一次返回的是结果，第二次返回的是所有数据-说以不用pass
    base_pass=map(lambda x: x.remove('') if '' in x else x,base_list)
    good_pass=map(lambda x: x.remove('') if '' in x else x,good_list)
    bad_pass=map(lambda x: x.remove('') if '' in x else x,bad_list)
    lost_pass=map(lambda x: x.remove('') if '' in x else x,lost_list)
    #删除空元素   
    base_pass_insert=map(lambda x:x.append('') if len(x)==1 else x,base_list)
    good_pass_insert=map(lambda x:x.append('') if len(x)==1 else x,good_list)
    bad_pass_insert=map(lambda x:x.append('') if len(x)==1 else x,bad_list)
    lost_pass_insert=map(lambda x:x.append('') if len(x)==1 else x,lost_list)
    
    #添加type,name
    base_pass_append=map(lambda x:base_list.append(x) if base_list!=[] else base_list,[['pid',str(pid[order])],['pname',pname[order]],['ptype',str(ptype[order])]])
    good_pass_append=map(lambda x:good_list.append(x) if good_list!=[] else good_list,[['pid',str(pid[order])],['pname',pname[order]],['ptype',str(ptype[order])]])
    bad_pass_append=map(lambda x:bad_list.append(x) if bad_list!=[] else bad_list,[['pid',str(pid[order])],['pname',pname[order]],['ptype',str(ptype[order])]])
    lost_pass_append=map(lambda x:lost_list.append(x) if lost_list!=[] else lost_list ,[['pid',str(pid[order])],['pname',pname[order]],['ptype',str(ptype[order])]])
    
     
    #转换为字典
    base_dict = {}
    good_dict = {}
    bad_dict = {}
    lost_dict = {}
    for item in base_list:
        base_dict[item[0]] = item[1]
   
    for item in good_list:
        good_dict[item[0]] = item[1]
   
    for item in bad_list:
        bad_dict[item[0]] = item[1]
    
    for item in lost_list:
        lost_dict[item[0]] = item[1]
       
    #转化为df-我发现字典写入会报错
       
    base_df=pd.DataFrame(pd.Series(base_dict)).T
    good_df=pd.DataFrame(pd.Series(good_dict)).T
    bad_df=pd.DataFrame(pd.Series(bad_dict)).T
    lost_df=pd.DataFrame(pd.Series(lost_dict)).T
       
    
    return  base_df,good_df,bad_df,lost_df
#==============================================================================

    


#综合插入函数 一级

def unionInsert(partner_data,order_start,order_end):
    import time
    import pandas as pd
    import random
    for  i in range(order_start,order_end):
        print  i,pname[i]
        #获child url list
        getChError='no'
        try:
            link=getChildLink(pname[i]) 
        except:        
            print 'the getChildLink page  get error'
            getChError='yes'
            time.sleep(random.uniform(300,600))
            link=[]
        print 'link gotted',getChError
         
         
        #获取二级页面内容html
        for url in link:
            try:        
                html=getHtml(url) #获取child页面的信息并解析   
            except:
                print 'the childinformation page get error'
                time.sleep(100)
                html=[]
            time.sleep(random.uniform(1,3))
            print 'html gotted %s'%(len(link))
            if html!=[]:
                #二级页面解析
                base_df,good_df,bad_df,lsot_df=getHtmlData(html,i)    
                #mongoInsert
                if len(base_df)>0:
                    mongoConfig('fes_credit_base',base_df)
                if len(good_df)>0:
                    mongoConfig('fes_credit_good',good_df)
                if len(bad_df)>0:
                    mongoConfig('fes_credit_bad',bad_df)
                if len(base_df)>0:
                    mongoConfig('fes_credit_lost',base_df)   
        
        #日志文件
        log_Dict={}
        log_Dict['order']=str(i)
        log_Dict['name']=pname[i]
        log_Dict['id']=str(pid[i])
        log_Dict['type']=str(ptype[i])
        log_Dict['link']=len(link)
        log_Dict['getChError']=getChError
        log_df=pd.DataFrame(pd.Series(log_Dict)).T
        mongoConfig('fes_credit_log',log_df)
        
        
    
        
import pandas as pd    
partner_data=pd.read_csv('data1.csv')           
pid= partner_data.partner_id.tolist()
pname = partner_data.partner_name.tolist()
ptype = partner_data.partner_type.tolist()
  

import sys
sys.path.append('spiderlog')          
unionInsert(partner_data,0,len(partner_data))            
            
        
        
        
    
        
