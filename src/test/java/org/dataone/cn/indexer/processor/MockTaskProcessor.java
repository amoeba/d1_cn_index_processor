package org.dataone.cn.indexer.processor;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;

public class MockTaskProcessor {

    
    private static Logger logger = Logger.getLogger("MockTaskProcessor");
    private static ExecutorService executor = Executors.newFixedThreadPool(3);
    private static final ReentrantLock lock = new ReentrantLock();
            
    public MockTaskProcessor() {
        // TODO Auto-generated constructor stub
    }
    
    public void processList() {
        logger.info("MockTaskProcessor: entering processList...");
        
        for(int i=0; i < 100; i++) {
            processOnThread(i);
        }
        
        logger.info("MockTaskProcessor: ... leaving processList");
    }

    void processOnThread(final int i) {
        
        Runnable r = new Runnable() {

            @Override
            public void run() {
                waitAndPrint(3000, i);
            }
            
        };
        executor.submit(r);
        System.out.println("       Submitted task " + i);

    }
    
    void waitAndPrint(long t, int i) {
        System.out.println("Starting task: " + i);
        try {
            Thread.sleep(t);
        } catch (InterruptedException e) {
            System.out.println("Runnable task " + i + " was interrupted, so finishing early...");
        }
        System.out.println(
                String.format("after %d millis, finishing task: %d", t, i));
    }
    
    public ExecutorService getExecutorService() {
        return executor;
    }
}
