#!/usr/bin/python
# -*- coding: utf-8 -*-
# TIPTOP 測試區 rebuild 開發版
from os.path import join
from os import walk
import sys
import os
#import smtplib
#import xlwt
#import xlrd
import datetime
import time
import numpy as np
import pandas as pd
import json
from sqlalchemy import create_engine
#from email.mime.image import MIMEImage
#from email.mime.multipart import MIMEMultipart
#from email.header import Header
#from email.mime.text import MIMEText
#from smtplib import SMTPException
import cx_Oracle
from multiprocessing import Process, Pool
import subprocess
import logging
# 注：設定環境編碼方式，可解決讀取資料庫亂碼問題
os.environ['NLS_LANG'] = 'AMERICAN_AMERICA.AL32UTF8'


class baseUtilsX():
    """baseUtils"""
    def __init__(self):
        self.connectObj = ""
        self.connCnt = 0
        self.cursorCnt = 0

    def initOracleConnect(self):
        oracle_tns = cx_Oracle.makedsn('192.168.1.1', 1521,g_env)
        if self.connCnt == 0:
            self.connectObj = cx_Oracle.connect('ds', 'ds', oracle_tns)
            self.connCnt += 1

    def getOracleConnect(self):
        self.initOracleConnect()
        return self.connectObj

    def closeOracleConnect(self, connectObj):
        connectObj.close()
        self.connCnt -= 1

    def getOracleCursor(self):
        self.initOracleConnect()
        self.cursorCnt += 1
        return self.connectObj.cursor()

    def closeOracleCursor(self, cursorObj):
        cursorObj.close()
        self.cursorCnt -= 1
        if self.cursorCnt == 0:
            print("will close conn")
            self.closeOracleConnect(self.connectObj)

    def selectFromDbTable(self, sql, argsDict):
        queryAnsList = []
        selectCursor = self.getOracleCursor()
        selectCursor.prepare(sql)
        queryAns = selectCursor.execute(None, argsDict)
        for ansItem in queryAns:
            queryAnsList.append(list(ansItem))

        self.closeOracleCursor(selectCursor)
        return queryAnsList


def long_time_comp(path,module_name,name):
    l_cmd = "fglrun /u1/topprod/tiptop/ds4gl2/bin/gen42m "+name+" tiptop"
    l_path=path+"/4gl"
    l_message = "[{modu}] RUN:{cmd}".format(modu=module_name.upper(),cmd=l_cmd)
    logger.info(l_message)
    completed = subprocess.run(l_cmd, cwd=l_path,shell=True,stdout=file)

def long_time_link(module_name,name):
    l_cmd = "fglrun /u1/topprod/tiptop/ds4gl2/bin/gen42r "+name+" tiptop"
    l_message = "[{modu}] RUN:{cmd}".format(modu=module_name.upper(),cmd=l_cmd)
    logger.info(l_message)
    completed = subprocess.run(l_cmd,shell=True,stdout=file)

def long_time_form(path,module_name,name):
    l_cmd = "r.f2 "+name
    l_path=path+"/4fd"
    #print(module_name,l_cmd)
    l_message = "[{modu}] RUN:{cmd}".format(modu=module_name.upper(),cmd=l_cmd)
    logger.info(l_message)
    completed = subprocess.run(l_cmd, cwd=l_path,shell=True,stdout=file)

#測試區產生ds.sch
def toptest_s2():
    l_cmd = "r.s2 ds"
    l_path=g_top+r"/schema"
    print(l_path,l_cmd)
    l_message = "Path:{path} run:{cmd}".format(path=l_path,cmd=l_cmd)
    logger.info(l_message)
    completed = subprocess.run(l_cmd, cwd=l_path,shell=True,stdout=file)

#測試區產生ds.sch
def toptest_link():
    l_cmd1 = "r.l2 lib"
    l_cmd2 = "r.l2 sub"
    l_cmd3 = "r.l2 qry"
    l_path=g_top
    #print(l_path,l_cmd1)
    l_message = "Path:{path} run:{cmd}".format(path=l_path,cmd=l_cmd1)
    logger.info(l_message)
    completed1 = subprocess.run(l_cmd1, cwd=l_path,shell=True,stdout=file)

    #print(l_path,l_cmd2)
    l_message = "Path:{path} run:{cmd}".format(path=l_path,cmd=l_cmd2)
    logger.info(l_message)
    completed2 = subprocess.run(l_cmd2, cwd=l_path,shell=True,stdout=file)

    #print(l_path,l_cmd3)
    l_message = "Path:{path} run:{cmd}".format(path=l_path,cmd=l_cmd3)
    logger.info(l_message)
    completed3 = subprocess.run(l_cmd3, cwd=l_path,shell=True,stdout=file)
    print("LINK finished...")
    logger.info("LINK finished...")

def gorebuild_agfgl(module_name):
    subpid = str(os.getpid())
    print("===================================================")
    print("Run gorebuild_fgl %s (%s)..." % (module_name, subpid))

    # 指定要列出所有檔案的目錄
    mypath = g_top+r"/"+module_name
    #print(mypath)
    # time.sleep(10)

    fglExt = r".4gl"
    ffdExt = r".4fd"
    dir_4gl = "4gl"
    dir_4fd = "4fd"

    # 遞迴列出所有檔案的絕對路徑
    for root, dirs, files in walk(mypath):
        #print("root=" + root+" find:"+str(root.find(dir_4gl)))
        if root.find(dir_4gl) > 0:
            #print("find 4gl:", root)
            # fulldir = join(root, dir)
            # for f in files:
            # if f.endswith(fglExt):
            # fullpath = join(subpid, ": ", root, f)
            # print(fullpath)
            os.chdir(root)
            for fgl_root, fgl_dirs, fgl_files in walk(root):
                #print("chang path => "+root)
                p1 = Pool(100)
                for f in fgl_files:
                    # print("4gl=>"+f)
                    (filename, extension) = os.path.splitext(f)
                    #print(filename, extension)

                    if extension == fglExt:
                        #l_cmd = "fglrun /u1/topprod/tiptop/ds4gl2/bin/gen42m "+filename+" tiptop"

                        p1.apply_async(long_time_comp, args=(mypath,module_name,filename))

                        #print(root+"$ "+l_cmd+' returncode:',
                        #     completed.returncode)
                p1.close()
                p1.join()

def gorebuild_cgfgl(module_name):
    subpid = str(os.getpid())
    print("===================================================")
    print("Run gorebuild_fgl %s (%s)..." % (module_name, subpid))

    # 指定要列出所有檔案的目錄
    mypath = g_cust+r"/"+module_name
    #print(mypath)
    # time.sleep(10)

    fglExt = r".4gl"
    ffdExt = r".4fd"
    dir_4gl = "4gl"
    dir_4fd = "4fd"

    # 遞迴列出所有檔案的絕對路徑
    for root, dirs, files in walk(mypath):
        dirs.sort()
        #print("root=" + root+" find:"+str(root.find(dir_4gl)))
        if root.find(dir_4gl) > 0:
            #print("find 4gl:", root)
            # fulldir = join(root, dir)
            # for f in files:
            # if f.endswith(fglExt):
            # fullpath = join(subpid, ": ", root, f)
            # print(fullpath)
            os.chdir(root)
            for fgl_root, fgl_dirs, fgl_files in walk(root):
                dirs.sort()
                #print("chang path => "+root)
                p1 = Pool(100)
                for f in fgl_files:
                    # print("4gl=>"+f)
                    (filename, extension) = os.path.splitext(f)
                    #print(filename, extension)

                    if extension == fglExt:
                        #l_cmd = "fglrun /u1/topprod/tiptop/ds4gl2/bin/gen42m "+filename+" tiptop"

                        p2.apply_async(long_time_comp, args=(mypath,module_name,filename))

                        #print(root+"$ "+l_cmd+' returncode:',
                        #     completed.returncode)
                p1.close()
                p1.join()

def gorebuild_link(module_name,prog_name):
    subpid = str(os.getpid())
    print("===================================================")
    print("Run gorebuild_link (%s)..." % (subpid))

    p2 = Pool(100)
    for filename in prog_name:
        p3.apply_async(long_time_link, args=(module_name,filename))

    p2.close()
    p2.join()

def gorebuild_affd(module_name):
    subpid = str(os.getpid())
    print("Run gorebuild_affd %s (%s)..." % (module_name, subpid))

    # 指定要列出所有檔案的目錄
    mypath = g_top+r"/"+module_name
    #print(mypath)
    # time.sleep(10)

    fglExt = r".4gl"
    ffdExt = r".4fd"
    dir_4gl = "4gl"
    dir_4fd = "4fd"
        # 遞迴列出所有檔案的絕對路徑
    for root, dirs, files in walk(mypath):
        dirs.sort()
        #print("root=" + root+" find:"+str(root.find(dir_4gl)))
        if root.find(dir_4fd) > 0:
            os.chdir(root)
            for fgl_root, fgl_dirs, fgl_files in walk(root):
                #print("chang path => "+root)
                p3 = Pool(200)
                for f in fgl_files:
                    # print("4gl=>"+f)
                    (filename, extension) = os.path.splitext(f)

                    if extension == ffdExt:
                        p3.apply_async(long_time_form, args=(mypath,module_name,filename))

                p3.close()
                p3.join()

def gorebuild_cffd(module_name):
    subpid = str(os.getpid())
    print("Run gorebuild_cffd %s (%s)..." % (module_name, subpid))

    # 指定要列出所有檔案的目錄
    mypath = g_cust+r"/"+module_name

    fglExt = r".4gl"
    ffdExt = r".4fd"
    dir_4gl = "4gl"
    dir_4fd = "4fd"

    # 遞迴列出所有檔案的絕對路徑
    for root, dirs, files in walk(mypath):
        dirs.sort()
        #print("root=" + root+" find:"+str(root.find(dir_4gl)))
        if root.find(dir_4fd) > 0:
            os.chdir(root)
            for fgl_root, fgl_dirs, fgl_files in walk(root):
                #print("chang path => "+root)
                p3 = Pool(200)
                for f in fgl_files:
                    # print("4gl=>"+f)
                    (filename, extension) = os.path.splitext(f)

                    if extension == ffdExt:
                        p3.apply_async(long_time_form, args=(mypath,module_name,filename))

                p3.close()
                p3.join()

#從資料庫抓取資料
def getData(execSQL,p_plant='ds'):
    #print("SQL :"+execSQL)
    #host = "192.168.1.1"  #資料庫ip
    host = g_hostname
    port = "1521"   #埠
    sid = g_env  #資料庫名稱
    dsn = cx_Oracle.makedsn(host, port, sid)
    #scott是資料使用者名稱，tiger是登入密碼（預設使用者名稱和密碼）
    db = cx_Oracle.connect(p_plant, p_plant, dsn)

    cur = db.cursor()
    cur.execute(execSQL)
    results = cur.fetchall()

	# 获取列名,将列名保存到row0列表
    Titles = []
    for col in cur.description:
        Titles.append(col[0])

    # 获取数据
    Results = []
    queryAnsList = []
    for result in results:
        #print(str(result[0]),type(result))
        queryAnsList.append(str(result[0]))
    Results = queryAnsList
    #print(Results)
    cur.close()
    db.close()

    return Results



#主程式
def main():
    print('Python start ======\n')

    #產生r.s2 ds
    #toptest_s2()

    #r.c2 clib csub cqry
    for i in rebuild_cust:
        print("Waiting for "+i+" subprocess done...")
        gorebuild_cgfgl(i)

    #r.c2 lib sub qry
    for i in rebuild_prod:
        print("Waiting for "+i+" subprocess done...")
        gorebuild_agfgl(i)

    #LINK lib sub qry
    toptest_link()

    #r.c2 標準程式
    for i in rebuild_aglist:
        print("Waiting for "+i+" subprocess done...")
        gorebuild_agfgl(i)

    #r.c2 客製程式
    for i in rebuild_cglist:
        print("Waiting for "+i+" subprocess done...")
        gorebuild_cgfgl(i)

    #r.l2 標準程式
    for i in rebuild_aglist:
        print("Waiting for "+i+" subprocess done...")
        sqlstr = "select zz01 from zz_file where zz08 NOT LIKE '%p_query%' AND zz011='{mod}' ORDER BY zz01".format(mod=i.upper())
        #print("SQL: "+sqlstr)
        Arrays = getData(sqlstr)
        #讀取模組下的程式
        if len(Arrays) > 0:
            gorebuild_link(i.upper(),Arrays)

    #r.l2 客製程式
    for i in rebuild_cglist:
        print("Waiting for "+i+" subprocess done...")
        sqlstr = "select zz01 from zz_file where zz08 NOT LIKE '%p_query%' AND zz011='{mod}' ORDER BY zz01".format(mod=i.upper())
        #print("SQL: "+sqlstr)
        Arrays = getData(sqlstr)
        #讀取模組下的程式
        if len(Arrays) > 0:
            gorebuild_link(i.upper(),Arrays)

    #r.f2 標準畫面
    for i in rebuild_aglist:
        print("Waiting for "+i+" subprocess done...")
        gorebuild_affd(i)

    #r.f2 客製畫面
    for i in rebuild_cglist:
        print("Waiting for "+i+" subprocess done...")
        gorebuild_cffd(i)

    print("All subprocess done.")

def test():
    print('Python start ======\n')

    #r.c2 客製程式
    #for i in rebuild_test:
    #    print("Waiting for "+i+" subprocess done...")
    #    gorebuild_cgfgl(i)
    #
    ##r.l2 客製程式
    #for i in rebuild_test:
    #    print("Waiting for "+i+" subprocess done...")
    #    sqlstr = "select zz01 from zz_file where zz08 NOT LIKE '%p_query%' AND zz011='{mod}' ORDER BY zz01".format(mod=i.upper())
    #    #print("SQL: "+sqlstr)
    #    Arrays = getData(sqlstr)
    #    #讀取模組下的程式
    #    if len(Arrays) > 0:
    #        gorebuild_link(i.upper(),Arrays)

    #r.f2 標準畫面
    for i in rebuild_aglist:
        print("Waiting for "+i+" subprocess done...")
        gorebuild_affd(i)

    #r.f2 客製畫面
    for i in rebuild_cglist:
        print("Waiting for "+i+" subprocess done...")
        gorebuild_cffd(i)

    print("All subprocess done.")

def end():
    print('Python  end  ======')

def log():
    logger = logging.getLogger(__name__)
    logger.setLevel(level = logging.INFO)
    handler = logging.FileHandler(filename)
    handler.setLevel(logging.INFO)
    #formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    formatter = logging.Formatter('%(asctime)s - %(message)s')
    handler.setFormatter(formatter)
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    logger.addHandler(handler)
    logger.addHandler(console)
    return logger

#環境變數
def usePlatform():
    sysstr = sys.platform
    global g_env
    global g_hostname
    global g_output
    global g_top
    global g_cust
    g_env = os.getenv("ORACLE_SID", default='toptest')
    g_hostname = os.getenv("HOSTNAME", default='172.20.3.22')
    g_output = os.getenv("TEMPDIR", default='/u1/out')
    g_top = os.getenv("TOP", default='/u1/toptest/tiptop')
    g_cust = os.getenv("CUST", default='/u1/toptest/topcust')
    # print(type(g_env))
    print("HOST:%s  ENV :%s  TEMPDIR:%s" % (g_hostname, g_env,g_output))
    return sysstr

if __name__ == '__main__':
    #環境偵測
    if sys.platform.startswith('win32'):
        # Do Windows stuff
        print("platform="+sys.platform)
    elif sys.platform.startswith('darwin'):
        # Do MacOS stuff
        print("platform="+sys.platform)
    elif sys.platform.startswith('linux'):
        # Do Linux stuff
        print("platform="+sys.platform)
    else:
        # Fallback
        print("platform=NONE")
    platform_env = usePlatform()
    print(platform_env)

    global filename
    mainpid = str(os.getpid())
    filename = g_output +"/rebuild_"+ mainpid+r"_ouput.log"
    filerun  = g_output +"/rebuild_"+ mainpid+r"_run.log"
    file = open(filerun, "w+")

    rebuild_prod = ['sub', 'qry', 'lib']
    rebuild_cust = ['csub', 'cqry', 'clib']
    rebuild_aglist = ['aap', 'aba', 'abg', 'abm',
                      'abx', 'aco', 'acs', 'adm',
                      'aec', 'aem', 'afa', 'agl',
                      'aic', 'aim', 'alm', 'amd',
                      'amm', 'amr', 'ams', 'anm',
                      'aoo', 'apc', 'apj', 'apm',
                      'aps', 'aqc', 'arm', 'art',
                      'asd', 'asf', 'ask', 'asm', 'asr',
                      'atm', 'aws', 'axc', 'axm',
                      'axr', 'axs', 'azz', 'gap',
                      'gfa', 'ggl', 'gis', 'gnm',
                      'gpm', 'gxc', 'gxr'
                      ]
    rebuild_cglist = ['cap', 'cba', 'cbg', 'cbm',
                      'cbx', 'cco', 'ccs', 'cdm',
                      'cec', 'cem', 'cfa', 'cgl',
                      'cic', 'cim', 'clm', 'cmd',
                      'cmm', 'cmr', 'cms', 'cnm',
                      'coo', 'cpc', 'cpj', 'cpm',
                      'cps', 'cqc', 'crm', 'crt',
                      'csd', 'csf', 'csk', 'csm', 'csr',
                      'ctm', 'cws', 'cxc', 'cxm',
                      'cxr', 'cxs', 'czz', 'cgap',
                      'cgfa','cggl','cgis','cgnm',
                      'cgpm','cgxc','cgxr']
    rebuild_test = ['coo','crm','czz']


    logger = log()
    logger.info("Start record rebuild log")
    #main()
    test()
    end()
    logger.info("End record rebuild log ====")

    print("")
    print("PROC LOG file in "+filename)
    print("RUN  LOG file in "+filerun)
