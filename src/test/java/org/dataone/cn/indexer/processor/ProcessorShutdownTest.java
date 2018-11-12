package org.dataone.cn.indexer.processor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.BeforeClass;
import org.junit.Test;


public class ProcessorShutdownTest {

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
    }

    @Test
    public void testShutdown() throws Exception {
        ExecutorService es = Executors.newFixedThreadPool(10);
        int i = 0;
        
        Map<Integer,Future<String>> futures = new HashMap<>();
        while (++i <= 50) {
            Thread.sleep(20);
            Future<String> f = es.submit(new LongCall(i));
            futures.put(i,f);
        }
        
        System.out.println("Shutting down the executor service... "+ System.currentTimeMillis());
        es.shutdown();
        
        System.out.println("Try to submit more tasks to shutdown executor... " + System.currentTimeMillis());
        try {
            while (i++ < 100) {
                Future<String> f = es.submit(new LongCall(i));
                futures.put(i, f);

            }
        } catch (Throwable t) {
            System.out.println("Exception from executor service while trying to submit tasks to a shutdown executor");
            t.printStackTrace();
            
        }
        
        System.out.println("Canceling cancelable tasks... "+ System.currentTimeMillis());
        
        Iterator<Integer> it = futures.keySet().iterator();
        List<Integer> canceled = new ArrayList<>();
        List<Integer> done = new ArrayList<>();
        while (it.hasNext()) {
            Integer key = it.next();
            Future<String> f = futures.get(key);
            if (f.isDone()) {
                System.out.println("Task " + key + " is done. "+ System.currentTimeMillis());
                done.add(key);
            }
            else {
                System.out.println("Task " + key + " is NOT done. "+ System.currentTimeMillis());
            
                if (f.cancel(false)) {
                    System.out.println("Task " + key + " successfully canceled. "+ System.currentTimeMillis());
                    canceled.add(key);
                } 
                else {
                    // could be complete or actively processing (or theoretically already canceled)
                    System.out.println("Task " + key + " NOT cancelable. "+ System.currentTimeMillis());
                }
            }
        }
        System.out.println("Sleeping 4000ms... "+ System.currentTimeMillis());
        Thread.sleep(4000);
        System.out.println("Starting a hard shutdown of executor... "+ System.currentTimeMillis());
        List<Runnable> tasks = es.shutdownNow();
        for (Runnable r : tasks) {
            System.out.println("Task " + ((LongCall)r).getID() + " was killed "+ System.currentTimeMillis());
        }
        System.out.println("Done. "+ System.currentTimeMillis());

    }


    
    class LongCall implements Callable {
        
        int id = -1;  
        LongCall(final int i) {
            this.id = i;
        }
        
        @Override
        public String call() {
          
            String threadName = Thread.currentThread().getName(); 
            System.out.println("Callable " + this.id + " is sleeping... "+ System.currentTimeMillis());
            for (int i=0; i<200; i++) {
                try {
                    Thread.currentThread().sleep(10);
                } catch (InterruptedException e) {
                    System.out.println("Callable " + this.id + " caught an interruption!!! "+ System.currentTimeMillis());
                }
            }
            System.out.println("Callable " + this.id + " is done. "+ System.currentTimeMillis());
            return threadName;
        }
        
        public int getID() {
            return this.id;
        }
        
    }

}
