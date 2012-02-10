package org.dataone.cn.indexer.convert;

/**
 * Basic value check for latitude entries.
 * 
 * @author vieglais
 * 
 */
public class SolrLatitudeConverter implements IConverter {

  /**
   * Given a string representation of a potential latitude value, ensure it is a
   * valid floating point number and check the range.
   * 
   * @param data
   *          The value to convert
   * 
   * @return String that is a valid representation of latitude.
   */
  public String convert(String data) {
    double v = 0.0d;
    try {
      v = Double.parseDouble(data.trim());
      if (v < -90.0d) {
        throw new NumberFormatException("Latitude < -90.0");
      }
      if (v > 90.0d) {
        throw new NumberFormatException("Latitude > 90.0");
      }
    } catch (NumberFormatException e) {
      return null;
    }
    return Double.toString(v);
  }
}
