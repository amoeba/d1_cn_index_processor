/**
 * 
 */
package org.dataone.cn.index;

import java.util.Date;
import java.util.Calendar;

/**
 * Simple class that contains the information necessary for a single indexing task.
 *
 */
public class IndexingTask {

  public String pid = null;
  public String sysMetaPath = null;
  public String objectPath = null;
  public Date dateSysmModified = null;
  public Date tstamp = null;
  
  public void IndexingTask() {
    tstamp = new Date();
  }
  
  public void IndexingTask(String id, String sysMeta, String object, Date tmod) {
    pid = id;
    sysMetaPath = sysMeta;
    objectPath = object;
    dateSysmModified = tmod;
    tstamp = new Date();
  }
  
  public void setPid(String pid) {
    this.pid = pid;
  }
  
  public void setSysMetaPath(String sysMetaPath) {
    this.sysMetaPath = sysMetaPath;
  }
  
  public void setObjectPath(String objectPath) {
    this.objectPath = objectPath;
  }
  
  public void setDateSysmModified(String tstamp) {
    long t = Long.parseLong(tstamp);
    dateSysmModified = new Date(t);
  }
  
}
