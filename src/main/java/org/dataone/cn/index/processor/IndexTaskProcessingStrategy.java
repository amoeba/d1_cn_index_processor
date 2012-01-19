package org.dataone.cn.index.processor;

import org.dataone.cn.index.task.IndexTask;

public interface IndexTaskProcessingStrategy {

    public void process(IndexTask task) throws Exception;
}
