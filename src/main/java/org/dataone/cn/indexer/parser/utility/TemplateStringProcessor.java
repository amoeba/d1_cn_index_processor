package org.dataone.cn.indexer.parser.utility;

import java.util.Map;

public class TemplateStringProcessor {

    public String process(String template, Map<String, String> valueMap) {
        String result = template;
        for (String key : valueMap.keySet()) {
            String value = valueMap.get(key);
            if (result.contains(key)) {
                result = result.replaceAll("\\[" + key + "\\]", value);
            }
        }
        return result;
    }
}
