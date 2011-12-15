package org.dataone.cn.index.processor;

import org.apache.commons.daemon.Daemon;
import org.apache.commons.daemon.DaemonContext;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

public class IndexTaskProcessorDaemon implements Daemon {

    private ApplicationContext context;
    private IndexTaskProcessorScheduler scheduler;

    @Override
    public void start() throws Exception {
        System.out.println("starting index task processor daemon...");
        context = new ClassPathXmlApplicationContext("processor-daemon-context.xml");
        scheduler = (IndexTaskProcessorScheduler) context.getBean("indexTaskProcessorScheduler");
        scheduler.start();
    }

    @Override
    public void stop() throws Exception {
        System.out.println("stopping index task processor daemon...");
        scheduler.stop();
    }

    @Override
    public void destroy() {

    }

    @Override
    public void init(DaemonContext arg0) throws Exception {

    }

}
