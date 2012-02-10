package org.dataone.cn.indexer.convert;

/**
 * Created by IntelliJ IDEA.
 * User: Porter
 * Date: 8/9/11
 * Time: 5:17 PM
 */

/**Compares values and returns "true" or "false" if value matches expected
 *
 */
public class BooleanMatchConverter implements IConverter{
    private String matchValue = null;

    public BooleanMatchConverter(String matchValue) {
        this.setMatchValue(matchValue);
    }

    public String convert(String data) {

        if(data == null || data.trim().length() <= 0){
            return "false";
        }
        else if(data.equals(matchValue)){

            return "true";
        }
        return "false";
    }

    public String getMatchValue() {
        return matchValue;
    }

    public void setMatchValue(String matchValue) {
        this.matchValue = matchValue;
    }
}
