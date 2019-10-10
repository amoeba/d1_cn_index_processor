package org.dataone.cn.indexer.annotation;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;
import java.util.Iterator;

import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.apache.commons.codec.EncoderException;

import org.apache.log4j.Logger;
import org.dataone.cn.index.util.PerformanceLogger;
import org.dataone.cn.indexer.parser.IDocumentSubprocessor;
import org.dataone.cn.indexer.parser.ISolrField;
import org.dataone.cn.indexer.parser.SubprocessorUtility;
import org.dataone.cn.indexer.solrhttp.SolrDoc;
import org.dataone.cn.indexer.solrhttp.SolrElementField;
import org.dataone.cn.indexer.AbstractStubMergingSubprocessor;
import org.dataone.cn.indexer.XmlDocumentUtility;
import org.springframework.beans.factory.annotation.Autowired;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

/**
 * A subprocessor that extracts semantic annotations from EML records and
 * materializes the superclass hierarchy for all extracted concepts using a
 * pre-defined set of ontologies. It is very similar to a
 * BaseXPathDocumentSubprocessor but a bit different in that it post-processes
 * results from the usual ISolrDoc.getFields() instead of merging them as-is.
 *
 * User: Mecum
 * Date: 2019/02/05
 * Time: 4:57 PM
 *
 */
public class EmlAnnotationSubprocessor extends AbstractStubMergingSubprocessor implements IDocumentSubprocessor {
    private static Logger log = Logger.getLogger(EmlAnnotationSubprocessor.class.getName());

    private PerformanceLogger perfLog = PerformanceLogger.getInstance();

    @Autowired
    private SubprocessorUtility processorUtility;

    private List<String> matchDocuments = null;

    private List<String> fieldsToMerge = new ArrayList<String>();

    private List<ISolrField> fieldList = new ArrayList<ISolrField>();

    private static XPathFactory xpathFactory = null;

    private static XPath xpath = null;

    static {
        xpathFactory = XPathFactory.newInstance();
        xpath = xpathFactory.newXPath();
    }

    /**
     * Returns true if subprocessor should be run against object
     *
     * @param formatId the the document to be processed
     * @return true if this processor can parse the formatId
     */
    public boolean canProcess(String formatId) {
        return matchDocuments.contains(formatId);
    }

    @Override
    public Map<String, SolrDoc> processDocument(String identifier, Map<String, SolrDoc> docs,
            InputStream is) throws Exception {
        log.debug(this.getClass().getName() + ".processDocument() called for identifier " + identifier);

        
        SolrDoc solrDoc = docs.get(identifier);

        if (solrDoc == null) {
            solrDoc = new SolrDoc();
            docs.put(identifier, solrDoc);
        }
        populateExpandedConcepts(identifier, solrDoc, is);
        
        return docs;
        
    }
    
    
    private void populateExpandedConcepts(String identifier, SolrDoc visitingSolrDoc, InputStream is) throws SAXException {
        
        Document doc = XmlDocumentUtility.generateXmlDocument(is);
        
        // Stores the set of expanded concepts per field
        Map<String, Set<String>> expandedFields = new HashMap<String, Set<String>>();

        // TODO: I think this is overly complex. Why am I writing four for loops?
        for (ISolrField solrField : fieldList) {
            long getFieldsStart = System.currentTimeMillis();

            try {
                List<SolrElementField> fields  = solrField.getFields(doc, identifier);

                for (SolrElementField field: fields) {
                    log.debug("Expanding concepts for " + field.getName() + ": " + field.getValue());
                    Map<String, Set<String>> expandedConcepts = OntologyModelService.getInstance().expandConcepts(field.getValue());

                    for (Map.Entry<String, Set<String>> expandedField: expandedConcepts.entrySet()) {
                        for (String value: expandedField.getValue()) {
                            if (!expandedFields.containsKey(field.getName())) {
                                expandedFields.put(field.getName(), new HashSet<String>());
                            }

                            expandedFields.get(field.getName()).add(value);
                        }
                    }
                }
            } catch (Exception e) {
                log.error("Failed to parse out expanded concepts from document. Logging and continuing...", e);
            }

            String fieldName = solrField.getName();
            perfLog.log("BaseXPathDocumentSubprocessor.processDocument() processing id "+identifier +" of field " + 
                    solrField.getClass().getSimpleName() + "(\"" + fieldName +"\").getFields()", 
                    System.currentTimeMillis() - getFieldsStart);
        }

        // Add the set of expanded concepts to the visiting Solr document
        log.debug("About to add expandedFields of size " + expandedFields.size());
        for (String field: expandedFields.keySet()) {
            log.debug("Adding field " + field + " to solrDoc");
            for (String concept: expandedFields.get(field)) {
                log.debug("  concept is " + concept);
                visitingSolrDoc.addField(new SolrElementField(field, concept));
            }
        }
    }

    /**
     * Merge updates with existing solr documents
     *
     * @param indexDocument
     * @return
     * @throws IOException
     * @throws EncoderException
     * @throws XPathExpressionException
     */
    public SolrDoc mergeWithIndexedDocument(SolrDoc indexDocument) throws IOException,
            EncoderException, XPathExpressionException {

        return processorUtility.mergeWithIndexedDocument(indexDocument, fieldsToMerge);
    }

	/**
	 * @return the processorUtility
	 */
	public SubprocessorUtility getProcessorUtility() {
		return processorUtility;
	}

	/**
	 * @param processorUtility the processorUtility to set
	 */
	public void setProcessorUtility(SubprocessorUtility processorUtility) {
		this.processorUtility = processorUtility;
	}

	/**
	 * @return the perfLog
	 */
	public PerformanceLogger getPerfLog() {
		return perfLog;
	}

	/**
	 * @param perfLog the perfLog to set
	 */
	public void setPerfLog(PerformanceLogger perfLog) {
		this.perfLog = perfLog;
	}


    public List<String> getMatchDocuments() {
        return matchDocuments;
    }

    public void setMatchDocuments(List<String> matchDocuments) {
        this.matchDocuments = matchDocuments;
    }

    public List<String> getFieldsToMerge() {
        return fieldsToMerge;
    }

    public void setFieldsToMerge(List<String> fieldsToMerge) {
        this.fieldsToMerge = fieldsToMerge;
    }

    public List<ISolrField> getFieldList() {
        return fieldList;
    }

    public void setFieldList(List<ISolrField> fieldList) {
        this.fieldList = fieldList;
        initExpression(xpath);
    }

    public void initExpression(XPath xpathObject) {
        for (ISolrField solrField : fieldList) {
            solrField.initExpression(xpathObject);
        }
    }

    @Override
    protected Map<String, SolrDoc> parseDocument(String identifier, InputStream source) throws Exception {
        
        Map<String, SolrDoc> docMap = new HashMap<>();
        SolrDoc solrDoc = new SolrDoc();
        docMap.put(identifier, solrDoc);
        return processDocument(identifier, docMap, source );

    }
}
