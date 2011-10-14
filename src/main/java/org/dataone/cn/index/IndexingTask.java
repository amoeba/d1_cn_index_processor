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
  public String fmtid = null;
  public String sysMetaPath = null;
  public String objectPath = null;
  public long dateSysmModified = 0;
  public Date tstamp = null;
  
  public void IndexingTask() {
    tstamp = new Date();
  }
  
  public void IndexingTask(String id, String fmtid, String sysMeta, String object, long tmod) {
    this.pid = id;
    this.fmtid = fmtid;
    this.sysMetaPath = sysMeta;
    this.objectPath = object;
    this.dateSysmModified = tmod;
    this.tstamp = new Date();
  }
  
  public void setPid(String pid) {
    this.pid = pid;
  }
  
  public void setFmtid(String fmtid) {
    this.fmtid = fmtid;
  }
  
  public void setSysMetaPath(String sysMetaPath) {
    this.sysMetaPath = sysMetaPath;
  }
  
  public void setObjectPath(String objectPath) {
    this.objectPath = objectPath;
  }
  
  public void setDateSysmModified(String tstamp) {
    dateSysmModified = Long.parseLong(tstamp);
    //dateSysmModified = new Date(t);
  }
  
}
