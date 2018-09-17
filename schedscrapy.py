import  os
import sched
import time
import datetime


### 简单的定时触发爬虫  
### 
def main():

    print("sched start...", time.time())
    schedule = sched.scheduler(time.time, time.sleep)
    while(True):
        print("开始：", time.time())
        now = datetime.datetime.now()
        isrun = False 

        if (now.hour == 22 ) :
            print("开始进行fund数据抓取 ")
            schedule.enter(3,0, func, ("scrapy crawl funlist", time.time()))
            schedule.enter(4,0, func, ("scrapy crawl fundNetvalHist", time.time()))
            schedule.run()
            isrun = True

        if (isrun):
            print("运行完，休眠22小时 ")
            time.sleep(22*60*60)
        else:
            print("休眠30分钟 ")
            time.sleep(30*60)

    print("end..", time.time())

def func(cmdstr, starttm):
    now = time.time()
    print("开始处理爬虫： ", cmdstr , " | output=", starttm, " 当前时间：" , now)
    os.system(cmdstr)
    print("爬虫结束： ", cmdstr , " 耗时：" , time.time() - now)



if __name__ == '__main__':
    main()