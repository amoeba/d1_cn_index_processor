package org.dataone.cn.indexer.parser.utility;

import org.apache.commons.lang.StringUtils;

public class LogicalOrPostProcessor {

    private static final String OR = "||";

    public LogicalOrPostProcessor() {

    }

    public String process(String value) {
        if (value.contains(OR)) {
            String[] partition = StringUtils.split(value, OR);
            if (partition.length > 2) {
                // only handling a single logical or at this time.
                return value;
            } else if (partition.length > 0 && !partition[0].isEmpty()) {
                return partition[0];
            } else if (partition.length > 1 && !partition[1].isEmpty()) {
                return partition[1];
            } else {
                return value;
            }
        } else {
            return value;
        }
    }
}
