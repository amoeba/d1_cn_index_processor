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
          break;
        case 2:
          task.setDateSysmModified(line);
          break;
        case 3:
          task.setSysMetaPath(basePath + line);
          break;
        case 4:
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
