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
package org.dataone.cn.index.task;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import javax.persistence.Transient;


/**
 * This class represents an index task for a resource map object. 
 * Besides the base index task, it has a new field which contains the ids of  
 * object referenced by the resource map.
 * @author tao
 *
 */
public class ResourceMapIndexTask extends IndexTask implements Serializable {

    private static final long serialVersionUID = 859612867797015876L;
   
    private List<String> referencedIds;
    

    public List<String> getReferencedIds() {
        return referencedIds;
    }


    public void setReferencedIds(List<String> referencedIds) {
        this.referencedIds = referencedIds;
    }
    
    /**
     * Copy the fields of the given task to this object
     * @param task
     */
    @Transient
    public void copy(IndexTask task) {
        if(task != null) {
            this.setId(task.getId());
            this.setVersion(task.getVersion());
            this.setPid(task.getPid());
            this.setFormatId(task.getFormatId());
            this.setSysMetadata(task.getSysMetadata());
            this.setObjectPath(task.getObjectPath());
            this.setDateSysMetaModified(task.getDateSysMetaModified());
            this.setNextExection(task.getNextExecution());
            this.setTryCount(task.getTryCount());
            this.setDeleted(task.isDeleted());
            this.setPriority(task.getPriority());
            this.setStatus(task.getStatus());
            this.setTaskModifiedDate(task.getTaskModifiedDate());
        }    
    }
    

    public byte[] serialize() throws IOException {
        
        ByteArrayOutputStream baos;
        ObjectOutputStream out = null;
        try {
            baos = new ByteArrayOutputStream();
            out = new ObjectOutputStream(baos);
            out.writeObject(this);
            out.flush();
            
            return baos.toByteArray();
        } finally {
            if (out != null)
                out.close();    
        }      
    }
    
    public static ResourceMapIndexTask deserialize(byte[] objectBytes) throws IOException, ClassNotFoundException {

        ObjectInputStream in = null;
        try {
            in = new ObjectInputStream(new ByteArrayInputStream(objectBytes));
            return (ResourceMapIndexTask) in.readObject();
        } finally {
            if (in != null) in.close();
        }
    }
}
