package org.dataone.cn.indexer.convert;

/**
  * User: Porter
 * Date: 7/26/11
 * Time: 6:00 PM
 */


/**Interface is used to convert values obtained through XPath to values consumable by indexing server.
 *
 */

public interface IConverter {
    /** Method to process or format data
     * *
     * @param  data String data input
     * @return processed textual data
     */
    public String convert(String data);
}
