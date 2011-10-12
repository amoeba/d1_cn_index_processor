package org.dataone.cn.index;

/**
 * 
 */


import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.List;
import java.util.Properties;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.commons.codec.EncoderException;
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
import org.dataone.cn.indexer.XPathDocumentParser;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.context.support.FileSystemXmlApplicationContext;
import org.w3c.dom.Document;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;


public class App {
  public static final String DOCUMENT_PARSERS = "documentParsers";
  private static ApplicationContext context = null;
  private String springConfigFile = null;
  private String taskListPath = "/tmp/indexing/index_tasks.txt";
  private String objectBasePath = "";
  public List<XPathDocumentParser> parserList = null;
  Log log = LogFactory.getLog(App.class);

  /**
   * 
   * @param args
   */
  public static void main(String[] args) {
    App app = new App();
/*    Options opps = app.getOptions();
    CommandLineParser parser = new PosixParser();
    CommandLine line = null;
    try {
      line = parser.parse(opps, args);

    } catch (ParseException exp) {
      System.out.println(exp.getMessage());
      printHelp(opps);
      System.exit(-1);
    }

    String configFile = line.getOptionValue("config");
    String metadir = line.getOptionValue("metadir");
    app.setSpringConfigFile(configFile); */
    // app.index(metadir);
    app.run();
  }

  public void loadConfig() {
    Properties props = new Properties();
    URL url = ClassLoader.getSystemResource("index_processor.properties");
    try {
      props.load(url.openStream());
      taskListPath = props.getProperty("taskListPath", taskListPath);
      objectBasePath = props.getProperty("objectBasePath", objectBasePath);
    } catch (IOException e) {
      log.info(e.getMessage());
    }
  }
  
  
  public IndexingTaskList loadTasks(String sourcePath) {
    IndexingTaskList tasks = new IndexingTaskList(sourcePath, objectBasePath);
    return tasks;
  }
  
  /**
   * Here the indexing tasks are run sequentially, which is not really necessary
   * for indexing data and science metadata entries. Resource Maps however, do
   * need to be processed one at a time, since such processing will require
   * updates to the entries referenced by the resource map.
   * @param metadir
   */
  public void run() {
    loadConfig();
    context = getContext();
    parserList = (List<XPathDocumentParser>) context.getBean(DOCUMENT_PARSERS);
    XPathDocumentParser parser = parserList.get(0);
    
    IndexingTaskList tasks = loadTasks(taskListPath);
    
    for (int i=0; i<tasks.size(); i++) {
      IndexingTask task = tasks.get(i);
      log.debug("PID, sys, object = " + task.pid + ", " + task.sysMetaPath +", " + task.objectPath);
      try {
        parser.processPID(task.pid, task.sysMetaPath, task.objectPath);
      } catch (Exception e) {
        //log.error(e.getMessage());
        log.error(e.getMessage());
      }
    }
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
    Option spring = OptionBuilder.hasArg()
        .withDescription("spring configuration file").create("config");
    Option files = OptionBuilder
        .isRequired()
        .hasArg()
        .withDescription(
            "Directory containing System Metadata files to be indexed")
        .create("metadir");
    // Option overrideResourceMapResolver =
    // OptionBuilder.isRequired().hasArg().withDescription("Overrides resource map resolver fully qualified class name of type org.dataone.cn.indexer.parser.IDocumentProvider").create("resourceMapResolverClass");
    spring.setRequired(false);
    files.setRequired(true);
    // overrideResourceMapResolver.setRequired(false);
    options = options.addOption(spring).addOption(files);// .addOption(overrideResourceMapResolver);

    return options;
  }

  /**
   * 
   * @return
   */
  public ApplicationContext getContext() {
    if (context == null) {
      // URL resource = this.getClass().getResource("application-context.xml");
      // System.out.println("resource.getFile() = " + resource.getFile());
      if (springConfigFile != null) {
        context = new FileSystemXmlApplicationContext(springConfigFile);
      } else {
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

}
