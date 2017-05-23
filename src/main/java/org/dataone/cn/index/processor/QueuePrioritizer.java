package org.dataone.cn.index.processor;

import java.util.HashMap;
import java.util.Map;
/**
 * A class that sets the priority of an item based on its
 * representation within a lookback window.  A lower 
 * representation raises its priority.
 * 
 * To avoid the first elements going through the prioritizer
 * from getting artificially lower priorities, the highest
 * priority is returned for the first 1/2 window size number
 * of elements.  (For example with window size of 100, and 
 * number of priorities 3, the first 50 will return a priority of 3.
 * 
 * @author rnahf
 *
 */
public class QueuePrioritizer {


    private Map<String, Integer> subCounts = new HashMap<>();
    
    public int windowSize = 100; // default
    private int priorities = 2;  // default
    private int headIndex = 0;   // starting value
    private int grandTotal = 0;  // starting value

    
    private String[] latest;
    
    public QueuePrioritizer(int lookback, int priorities) {
        this. windowSize = lookback;
        this.priorities = priorities;
        this. latest = new String[windowSize];

    }
    
    public float pushNext(String group) {
    
        grandTotal++;
        if (subCounts.containsKey(group)) {
            subCounts.put(group, subCounts.get(group).intValue() + 1);
        } else {
            subCounts.put(group, 1);
        }
        
        String bumped = addToList(group);
        
        if (bumped != null) {
            subCounts.put(bumped, subCounts.get(bumped).intValue() - 1);
            grandTotal--;
        }

        // calculate priority
        // (1- %group) * number of bins

        // to avoid  
        if (grandTotal*2 < windowSize) 
            return (float)priorities;
        
        int subCount = subCounts.get(group);
        return ((grandTotal - subCount) * priorities / (float)grandTotal) + 1.0f;
        
    }
    

    private String addToList(String group) {
        String bumped = null;
        if (latest[headIndex] != null) {
            bumped = latest[headIndex];
        }
        latest[headIndex++] = group;
        // clockwork behavior
        if (headIndex >= windowSize) 
            headIndex = 0;
        
        return bumped;
    }

//    private int actualWindowSize() {     
//        return latest.length;
//    }
    
    

    
    

}
