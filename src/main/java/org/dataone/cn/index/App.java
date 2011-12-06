package org.dataone.cn.index;

/**
 * 
 */

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.Properties;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dataone.cn.index.processor.IndexTaskProcessor;
import org.dataone.cn.indexer.XPathDocumentParser;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.context.support.FileSystemXmlApplicationContext;

public class App {
    public static String DOCUMENT_PARSERS = "documentParsers";
    public static String LOCKFILE = "/tmp/indexing/indexlock.lck";
    public static String TSTAMPFILE = "/tmp/indexing/indextstamp.txt";
    private static ApplicationContext context = null;
    private String springConfigFile = "/etc/dataone/indexing/application-context.xml";
    private String processingProperties = "/etc/dataone/indexing/index_processor.properties";
    private String taskListPath = "/tmp/indexing/index_tasks.txt";
    private String objectBasePath = "";
    public List<XPathDocumentParser> parserList = null;
    Log log = LogFactory.getLog(App.class);

    /**
     * 
     * @param args
     */
    public static void main(String[] args) {

        File alock = new File(LOCKFILE);
        if (alock.exists()) {
            System.err.println("ERROR: Lock file " + LOCKFILE + " exists.");
            System.exit(1);
        }

        App app = new App();
        Options opps = app.getOptions();
        CommandLineParser parser = new PosixParser();
        CommandLine line = null;
        try {
            line = parser.parse(opps, args);

        } catch (ParseException exp) {
            System.out.println(exp.getMessage());
            printHelp(opps);
            System.exit(-1);
        }

        try {
            FileWriter flock = new FileWriter(LOCKFILE);
            flock.write("locked at: " + new Date().toString());
            flock.close();
            alock.deleteOnExit();
        } catch (IOException e) {
            System.err.println("ERROR: Could not create lock file " + LOCKFILE);
            System.exit(2);
        }

        String configFile = line.getOptionValue("config",
                "/etc/dataone/indexing/application-context.xml");
        String taskFile = line.getOptionValue("tasks", "/tmp/indexing/index_tasks.txt");
        String base = line.getOptionValue("base", "");
        // Load spring configuration
        app.setSpringConfigFile(configFile);
        // Load properties from the configuration file
        app.loadConfig();
        // Command line options override the properties file values
        app.setTaskListPath(taskFile);
        app.setObjectBasePath(base);
        long youngestSysMeta = app.run();
        if (youngestSysMeta >= 0) {
            try {
                FileWriter fts = new FileWriter(TSTAMPFILE);
                fts.write(youngestSysMeta + "\n");
                fts.close();
            } catch (IOException e) {
                System.err.println("ERROR: Could not create timestamp file " + TSTAMPFILE);
            }
        }
    }

    /**
     * Load options from properties file
     */
    public void loadConfig() {
        Properties props = new Properties();
        try {
            props.load(new FileInputStream(processingProperties));
            taskListPath = props.getProperty("taskListPath", taskListPath);
            objectBasePath = props.getProperty("objectBasePath", objectBasePath);
        } catch (IOException e) {
            log.info(e.getMessage());
        }
    }

    /**
     * Load a list of tasks from the task file. TODO: only add tasks that were
     * modified after the last run
     * 
     * @param sourcePath
     * @return
     */
    public IndexingTaskList loadTasks(String sourcePath) {
        IndexingTaskList tasks = new IndexingTaskList(sourcePath, objectBasePath);
        return tasks;
    }

    // public void populateNodeMap() {
    //
    // NodeList nodes = null;
    // CNode cnode = new CNode("https://cn-dev.dataone.org/cn");
    // try{
    // nodes = cnode.listNodes();
    // } catch (Exception e) {
    // log.error(e.getMessage());
    // }
    // for (int i=0; i < nodes.sizeNodeList(); i++) {
    // Node node = nodes.getNode(i);
    // String nodeid = node.getIdentifier().getValue();
    // log.info("Node id = " + nodeid + " Name = " + node.getName());
    // nodeMap.put(nodeid, node);
    // }
    // }

    /**
     * Here the indexing tasks are run sequentially, which is not really
     * necessary for indexing data and science metadata entries. Resource Maps
     * however, do need to be processed one at a time, since such processing
     * will require updates to the entries referenced by the resource map.
     * 
     * @param metadir
     */
    public long run() {
        context = getContext();
        parserList = (List<XPathDocumentParser>) context.getBean(DOCUMENT_PARSERS);
        XPathDocumentParser parser = parserList.get(0);

        IndexingTaskList tasks = loadTasks(taskListPath);

        // -1 indicates nothing changed so don't update the timestamp.
        long youngestTask = -1;

        for (int i = 0; i < tasks.size(); i++) {
            IndexingTask task = tasks.get(i);
            if (task.dateSysmModified > youngestTask) {
                youngestTask = task.dateSysmModified;
            }
            log.info("Processing PID, sys, object = " + task.pid + ", " + task.sysMetaPath + ", "
                    + task.objectPath);
            try {
                // if
                // (task.pid.equals("__test_object_valid_eml__sciMD-eml-201-NoLastLForCR__"))
                // {
                parser.processID(task.pid, task.sysMetaPath, task.objectPath);
                log.info("Processing complete for PID = " + task.pid);
                // }
            } catch (Exception e) {
                // log.error(e.getMessage());
                log.info("Processing failed for PID = " + task.pid);
                log.error(e.getMessage());
            }
        }
        return youngestTask;
    }

    public void run2() {
        context = getContext();
        IndexTaskProcessor processor = (IndexTaskProcessor) context.getBean("indexTaskProcessor");
        processor.processIndexTaskQueue();
    }

    /**
     * 
     * @param options
     */
    private static void printHelp(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("indexer", options);
    }

    /**
     * 
     * @return
     */
    public Options getOptions() {
        Options options = new Options();
        Option spring = OptionBuilder.hasArg().withDescription("spring configuration file")
                .create("config");
        Option files = OptionBuilder
                .hasArg()
                .withDescription("Full path to the tasks list file (/tmp/indexing/index_tasks.txt)")
                .create("tasks");
        Option base = OptionBuilder.hasArg()
                .withDescription("Filesystem root that config and tasks are relative to (null)")
                .create("base");
        spring.setRequired(false);
        files.setRequired(false);
        options = options.addOption(spring).addOption(files).addOption(base);
        return options;
    }

    /**
     * 
     * @return
     */
    public ApplicationContext getContext() {
        if (context == null) {
            try {
                context = new FileSystemXmlApplicationContext(springConfigFile);
            } catch (Exception e) {
                log.info(e.getMessage());
                log.info("Falling back to configuration included in jar file.");
                context = new ClassPathXmlApplicationContext("application-context.xml");
            }
        }
        return context;
    }

    /**
     * 
     * @param springConfigFile
     */
    public void setSpringConfigFile(String springConfigFile) {
        this.springConfigFile = springConfigFile;
    }

    public void setTaskListPath(String taskListPath) {
        this.taskListPath = taskListPath;
    }

    public void setObjectBasePath(String objectBasePath) {
        this.objectBasePath = objectBasePath;
    }

}
