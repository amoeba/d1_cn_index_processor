/**
 * This work was created by participants in the DataONE project, and is
 * jointly copyrighted by participating institutions in DataONE. For 
 * more information on DataONE, see our web site at http://dataone.org.
 *
 *   Copyright ${year}
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and 
 * limitations under the License.
 * 
 * $Id$
 */

package org.dataone.cn.index;

import java.util.Date;

/**
 * Candidate for removal - unused - replaced by IndexTask.
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
