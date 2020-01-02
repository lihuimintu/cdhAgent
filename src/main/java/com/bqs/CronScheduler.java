package com.bqs;

import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;

public class CronScheduler {
    public static void main(String[] args) throws Exception {
        JobDetail jobDetail = JobBuilder.newJob(HandleJob.class)
                .withIdentity("myJob").build();
        Trigger trigger = TriggerBuilder.newTrigger()
                .withIdentity("cronTrigger")
                //cron表达式 这里定义是每5分钟开始执行
                .withSchedule(CronScheduleBuilder.cronSchedule("0 0/1 * * * ? *"))
                .build();
        SchedulerFactory factory = new StdSchedulerFactory();
        //创建调度器
        Scheduler scheduler = factory.getScheduler();
        //启动调度器
        scheduler.start();
        //jobDetail和trigger加入调度
        scheduler.scheduleJob(jobDetail, trigger);
    }
}
