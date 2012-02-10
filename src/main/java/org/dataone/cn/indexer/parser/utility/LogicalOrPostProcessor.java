package org.dataone.cn.indexer.parser.utility;

import org.codehaus.plexus.util.StringUtils;

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
            } else if (!partition[0].isEmpty()) {
                return partition[0];
            } else if (!partition[1].isEmpty()) {
                return partition[1];
            } else {
                return value;
            }
        } else {
            return value;
        }
    }
}
