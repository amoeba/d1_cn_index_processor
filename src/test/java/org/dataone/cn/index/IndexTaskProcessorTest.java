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
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.hazelcast.config.ClasspathXmlConfig;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

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

    private HazelcastInstance hzMember;

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
        IndexTask it = new IndexTask(smd, "index-processor-test-object-path");
        it.setStatus(status);
        repo.save(it);
        return it;
    }

    @Before
    public void setUp() throws Exception {

        Config hzConfig = new ClasspathXmlConfig("org/dataone/configuration/hazelcast.xml");

        System.out.println("Hazelcast Group Config:\n" + hzConfig.getGroupConfig());
        System.out.print("Hazelcast Maps: ");
        for (String mapName : hzConfig.getMapConfigs().keySet()) {
            System.out.print(mapName + " ");
        }
        System.out.println();
        hzMember = Hazelcast.init(hzConfig);
        System.out.println("Hazelcast member hzMember name: " + hzMember.getName());
    }

    @After
    public void tearDown() throws Exception {
        Hazelcast.shutdownAll();
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