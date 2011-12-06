package org.dataone.cn.index;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Date;
import java.util.UUID;

import junit.framework.Assert;

import org.apache.log4j.Logger;
import org.dataone.cn.index.processor.IndexTaskProcessor;
import org.dataone.cn.index.task.IndexTask;
import org.dataone.cn.index.task.IndexTaskRepository;
import org.dataone.cn.indexer.XPathDocumentParser;
import org.dataone.service.types.v1.Checksum;
import org.dataone.service.types.v1.Identifier;
import org.dataone.service.types.v1.ObjectFormatIdentifier;
import org.dataone.service.types.v1.Subject;
import org.dataone.service.types.v1.SystemMetadata;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "test-context.xml" })
public class IndexTaskProcessorTest {

    private static Logger logger = Logger.getLogger(IndexTaskProcessorTest.class.getName());

    @Autowired
    private IndexTaskRepository repo;

    @Autowired
    private IndexTaskProcessor proc;

    @Autowired
    private ArrayList<XPathDocumentParser> documentParsers;

    @Test
    public void testInjection() {
        Assert.assertNotNull(repo);
        Assert.assertNotNull(proc);
        Assert.assertNotNull(documentParsers);
    }

    @Test
    public void testProcessing() {
        Assert.assertTrue(true);
        String pid = "proc task" + UUID.randomUUID().toString();
        IndexTask it = saveIndexTask(pid, IndexTask.STATUS_NEW);
        Long itId = it.getId();
        it = repo.findOne(itId);
        Assert.assertNotNull(it);
        Assert.assertTrue(pid.equals(it.getPid()));

        proc.processIndexTaskQueue();

        Assert.assertFalse(repo.exists(itId));

    }

    private IndexTask saveIndexTask(String pid, String status) {
        return saveIndexTaskWithStatus(pid, status);
    }

    private IndexTask saveIndexTaskWithStatus(String pid, String status) {
        SystemMetadata smd = buildTestSysMetaData(pid);
        IndexTask it = new IndexTask(smd, null);
        it.setStatus(status);
        repo.save(it);
        return it;
    }

    public SystemMetadata buildTestSysMetaData(String pidValue) {
        SystemMetadata systemMetadata = new SystemMetadata();

        Identifier identifier = new Identifier();
        identifier.setValue(pidValue);
        systemMetadata.setIdentifier(identifier);

        ObjectFormatIdentifier fmtid = new ObjectFormatIdentifier();
        fmtid.setValue("proc-test-format");
        systemMetadata.setFormatId(fmtid);

        systemMetadata.setSerialVersion(BigInteger.TEN);
        systemMetadata.setSize(BigInteger.TEN);
        Checksum checksum = new Checksum();
        checksum.setValue("V29ybGQgSGVsbG8h");
        checksum.setAlgorithm("SHA-1");
        systemMetadata.setChecksum(checksum);

        Subject rightsHolder = new Subject();
        rightsHolder.setValue("DataONE");
        systemMetadata.setRightsHolder(rightsHolder);

        Subject submitter = new Subject();
        submitter.setValue("Kermit de Frog");
        systemMetadata.setSubmitter(submitter);

        systemMetadata.setDateSysMetadataModified(new Date());
        return systemMetadata;
    }
}