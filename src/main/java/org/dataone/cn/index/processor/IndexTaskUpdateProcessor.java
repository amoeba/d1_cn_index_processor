package org.dataone.cn.index.processor;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.ArrayList;

import org.apache.log4j.Logger;
import org.dataone.cn.index.task.IndexTask;
import org.dataone.cn.index.task.IndexTaskRepository;
import org.dataone.cn.indexer.XPathDocumentParser;
import org.springframework.beans.factory.annotation.Autowired;

public class IndexTaskUpdateProcessor implements IndexTaskProcessingStrategy {

    private static Logger logger = Logger.getLogger(IndexTaskUpdateProcessor.class.getName());

    @Autowired
    private ArrayList<XPathDocumentParser> documentParsers;

    @Autowired
    private IndexTaskRepository repo;

    public void process(IndexTask task) throws Exception {
        XPathDocumentParser parser = getXPathDocumentParser();
        InputStream smdStream = new ByteArrayInputStream(task.getSysMetadata().getBytes());
        parser.process(task.getPid(), smdStream, task.getObjectPath());
    }

    private XPathDocumentParser getXPathDocumentParser() {
        return documentParsers.get(0);
    }
}
