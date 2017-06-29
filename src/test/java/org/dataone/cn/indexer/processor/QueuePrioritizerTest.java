package org.dataone.cn.indexer.processor;

import static org.junit.Assert.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.dataone.cn.index.processor.QueuePrioritizer;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class QueuePrioritizerTest {

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
    }

    @Before
    public void setUp() throws Exception {
    }

    /**
     * 
     */
    @Test
    public void testPushNext_SingleDominantGroup() {
        QueuePrioritizer qp = new QueuePrioritizer(100, 2);
        
        for (int i=0; i<200; i++) {
            float priority = qp.pushNext("A");
            System.out.println(String.format("%2d. %2.2f %d: added: %s", i, priority, (int)priority, "A"));
        }
        
        for (int i=200; i<220; i+=2) {
            float priority = qp.pushNext("B");
            System.out.println(String.format("%2d. %2.2f %d: added: %s", i, priority, (int)priority, "B"));
            priority = qp.pushNext("A");
            System.out.println(String.format("%2d. %2.2f %d: added: %s", i+1, priority, (int)priority, "A"));
        }
    }
    
    @Test
    public void testPushNext_MixedDominantGroup() {
        QueuePrioritizer qp = new QueuePrioritizer(50, 3);
        
        for (int i=0; i<200; i+=2) {
            float priority = qp.pushNext("A");
            System.out.println(String.format("%2d. %2.2f %d: added: %s", i, priority, (int)priority, "A"));
            priority = qp.pushNext("BB");
            System.out.println(String.format("%2d. %2.2f %d: added: %s", i+1, priority, (int)priority, "BB"));

        }
        
        for (int i=200; i<250; i+=2) {
            float priority = qp.pushNext("CCC");
            System.out.println(String.format("%2d. %2.2f %d: added: %s", i, priority, (int)priority, "CCC"));
            priority = qp.pushNext("BB");
            System.out.println(String.format("%2d. %2.2f %d: added: %s", i+1, priority, (int)priority, "BB"));
        }
    }
    
    
    @Test
    public void testPushNext_3Waves() {
        QueuePrioritizer qp = new QueuePrioritizer(100, 5);
        
        for (int i=0; i<30; i+=1) {
            float priority = qp.pushNext("A");
            System.out.println(String.format("%2d. %2.2f %d: added: %s", i, priority, (int)priority, "A"));
        }
        for (int i=30; i<60; i+=1) {
            float priority = qp.pushNext("BB");
            System.out.println(String.format("%2d. %2.2f %d: added: %s", i, priority, (int)priority, "BB"));
        }
        for (int i=60; i<90; i+=1) {
            float priority = qp.pushNext("CCC");
            System.out.println(String.format("%2d. %2.2f %d: added: %s", i, priority, (int)priority, "CCC"));
        }
       
    }
    
    @Test
    public void testOnSyncOrder() throws IOException {
        QueuePrioritizer qp = new QueuePrioritizer(100, 5);
        
        InputStream is = this.getClass().getClassLoader().getResourceAsStream("org/dataone/cn/indexer/processor/syncDistribution.csv");
        BufferedReader r = new BufferedReader(new InputStreamReader(is,"UTF-8"));
        String line = null;
        int i = 0;
        float lastPriority = 5;
        while ((line = r.readLine()) != null) {
            float priority = qp.pushNext(line);
            System.out.println(String.format("%6d. [%2.2f] %d: added: %s  %s",i++, priority, (int)priority, line, (int)priority > (int)lastPriority ? "----------" : ""));
            lastPriority = priority;
        }
    }

    
}
