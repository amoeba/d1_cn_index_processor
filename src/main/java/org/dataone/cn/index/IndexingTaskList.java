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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class IndexingTaskList extends ArrayList<IndexingTask> {

  // Path offset to files listed in the task list. Normally blank, this
  // is used for testing when mounting the remote file system at another root.
  private static String basePath = "";
  Log log = LogFactory.getLog(App.class);

  /**
   * Load indexing tasks from the temporary hack textfile
   * 
   * @param sourcePath
   *          File system path to the taks list.
   */
  public IndexingTaskList(String sourcePath) {
    loadTasks(sourcePath);
  }
  
  public IndexingTaskList(String sourcePath, String basePath) {
    if (basePath != null) {
      log.info("Initializing with basePath = " + basePath);
      this.basePath = basePath;
    }
    loadTasks(sourcePath);
  }

  
  public void loadTasks(String sourcePath) {
    File sourceFile = new File(sourcePath);
    try {
      List<String> lines = org.apache.commons.io.FileUtils
          .readLines(sourceFile);

      int lineCount = 0;
      IndexingTask task = null;
      for (String line : lines) {
        if (line.startsWith("TIMESTAMP")) {
          continue;
        }
        if (task == null || line.trim().equals("::")) {
          if (task != null) {
            this.add(task);
          }
          task = new IndexingTask();
          lineCount = 0;
          continue;
        }

        lineCount++;
        switch (lineCount) {
        case 0:
          break;
        case 1:
          task.setPid(line);
          //log.debug("TASK pid=" + line);
          break;
        case 2:
          task.setFmtid(line);
          break;
        case 3:
          task.setDateSysmModified(line);
          break;
        case 4:
          task.setSysMetaPath(basePath + line);
          break;
        case 5:
          if (!line.equals("null") & line != null) {
            task.setObjectPath(basePath + line);
          }
          break;
        }
      }
    } catch (IOException e) {
      log.error(e.getMessage());
    }    
  }

}
