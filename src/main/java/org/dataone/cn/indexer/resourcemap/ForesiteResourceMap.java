package org.dataone.cn.indexer.resourcemap;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.input.ReaderInputStream;
import org.apache.commons.lang.NotImplementedException;
import org.dataone.cn.index.processor.IndexVisibilityDelegateHazelcastImpl;
import org.dataone.cn.indexer.solrhttp.SolrDoc;
import org.dataone.cn.indexer.solrhttp.SolrElementField;
import org.dataone.ore.ResourceMapFactory;
import org.dataone.service.types.v1.Identifier;
import org.dspace.foresite.OREException;
import org.dspace.foresite.OREParserException;
import org.w3c.dom.DOMException;
import org.w3c.dom.Document;
import org.w3c.dom.bootstrap.DOMImplementationRegistry;
import org.w3c.dom.ls.DOMImplementationLS;
import org.w3c.dom.ls.LSException;

public class ForesiteResourceMap implements ResourceMap {
    /* Class contants */
    private static final String RESOURCE_MAP_FORMAT = "http://www.openarchives.org/ore/terms";

    /* Instance variables */
    private String identifier = null;
    private HashMap<String, ForesiteResourceEntry> resourceMap = null;

    private IndexVisibilityDelegate indexVisibilityDelegate = new IndexVisibilityDelegateHazelcastImpl();

    public ForesiteResourceMap() {

    }

    private SolrDoc _mergeMappedReference(ResourceEntry resourceEntry, SolrDoc mergeDocument) {
        if (mergeDocument.hasField(SolrElementField.FIELD_ID) == false) {
            mergeDocument.addField(new SolrElementField(SolrElementField.FIELD_ID, resourceEntry
                    .getIdentifier()));
        }

        for (String documentedBy : resourceEntry.getDocumentedBy()) {
            if (mergeDocument
                    .hasFieldWithValue(SolrElementField.FIELD_ISDOCUMENTEDBY, documentedBy) == false) {
                mergeDocument.addField(new SolrElementField(SolrElementField.FIELD_ISDOCUMENTEDBY,
                        documentedBy));
            }
        }

        for (String documents : resourceEntry.getDocuments()) {
            if (mergeDocument.hasFieldWithValue(SolrElementField.FIELD_DOCUMENTS, documents) == false) {
                mergeDocument.addField(new SolrElementField(SolrElementField.FIELD_DOCUMENTS,
                        documents));
            }
        }

        for (String resourceMap : resourceEntry.getResourceMaps()) {
            if (mergeDocument.hasFieldWithValue(SolrElementField.FIELD_RESOURCEMAP, resourceMap) == false) {
                mergeDocument.addField(new SolrElementField(SolrElementField.FIELD_RESOURCEMAP,
                        resourceMap));
            }
        }

        mergeDocument.setMerged(true);

        return mergeDocument;
    }

    private void _init(InputStream is) throws OREException, URISyntaxException,
            UnsupportedEncodingException, OREParserException

    {
        /* Creates the indentifier map from the doc */
        Map<Identifier, Map<Identifier, List<Identifier>>> tmpResourceMap = ResourceMapFactory
                .getInstance().parseResourceMap(is);

        //this.setMappedReferences(new HashSet<ResourceEntry>());

        /* Gets the top level identifier */
        Identifier identifier = tmpResourceMap.keySet().iterator().next();
        this.setIdentifier(identifier.getValue());

        /* Gets the to identifier map */
        Map<Identifier, List<Identifier>> identiferMap = tmpResourceMap.get(identifier);

        this.resourceMap = new HashMap<String, ForesiteResourceEntry>();

        for (Map.Entry<Identifier, List<Identifier>> entry : identiferMap.entrySet()) {
            ForesiteResourceEntry documentsResourceEntry = resourceMap.get(entry.getKey()
                    .getValue());

            if (documentsResourceEntry == null) {
                documentsResourceEntry = new ForesiteResourceEntry(entry.getKey().getValue(), this);
                resourceMap.put(entry.getKey().getValue(), documentsResourceEntry);
            }

            for (Identifier documentedByIdentifier : entry.getValue()) {
                documentsResourceEntry.addDocuments(documentedByIdentifier.getValue());

                ForesiteResourceEntry documentedByResourceEntry = resourceMap
                        .get(documentedByIdentifier.getValue());

                if (documentedByResourceEntry == null) {
                    documentedByResourceEntry = new ForesiteResourceEntry(
                            documentedByIdentifier.getValue(), this);

                    resourceMap.put(documentedByIdentifier.getValue(), documentedByResourceEntry);
                }

                documentedByResourceEntry.addDocumentedBy(entry.getKey().getValue());
            }
        }
    }

    public ForesiteResourceMap(String doc) throws OREException, URISyntaxException,
            OREParserException, IOException {
        InputStream is = new ReaderInputStream(new StringReader(doc));

        try {
            _init(is);

        } finally {
            is.close();
        }
    }

    public ForesiteResourceMap(InputStream is) throws UnsupportedEncodingException, OREException,
            URISyntaxException, OREParserException {
        _init(is);
    }

    public ForesiteResourceMap(Document doc) throws DOMException, LSException, ClassCastException,
            OREException, URISyntaxException, OREParserException, IOException,
            ClassNotFoundException, InstantiationException, IllegalAccessException {
        this(((DOMImplementationLS) (DOMImplementationRegistry.newInstance()
                .getDOMImplementation("LS"))).createLSSerializer().writeToString(doc));
    }

    public static boolean representsResourceMap(String formatId) {
        return RESOURCE_MAP_FORMAT.equals(formatId);
    }

    @Override
    public Set<ResourceEntry> getMappedReferences() {
        /* Builds a set for references that are visible in solr doc index and
         * are not the resource map id */
        HashSet<ResourceEntry> resourceEntries = new HashSet<ResourceEntry>();

        for (ResourceEntry resourceEntry : this.resourceMap.values()) {
            Identifier pid = new Identifier();
            pid.setValue(resourceEntry.getIdentifier());
            if (indexVisibilityDelegate.isDocumentVisible(pid)) {
                if (resourceEntry.getIdentifier().equals(this.getIdentifier()) == false) {
                    resourceEntries.add(resourceEntry);
                }
            }
        }

        /* Return the set of resource entries */
        return resourceEntries;
    }

    @Override
    public Set<String> getContains() {
        Set<String> contains = new HashSet<String>();

        for (String id : this.resourceMap.keySet()) {
            contains.add(id);
        }

        for (ResourceEntry resourceEntry : this.resourceMap.values()) {
            contains.add(resourceEntry.getIdentifier());
        }

        return contains;
    }

    void setContains(Set<String> contains) {
        /* Not sure if this is required by other parts of the code, may remove 
         * from the interface */

        throw new NotImplementedException();
    }

    void setMappedReferences(Set<ResourceEntry> mappedReferences) {
        /* Not sure if this is required by other parts of the code, may remove 
         * from the interface */
        //this.resourceEntries = mappedReferences;

        throw new NotImplementedException();
    }

    @Override
    public List<String> getAllDocumentIDs() {
        List<String> docIds = new LinkedList<String>();

        /* Adds the map identifier */
        docIds.add(this.getIdentifier());

        /* Adds the mapped references */
        for (ResourceEntry resourceEntry : getMappedReferences()) {
            docIds.add(resourceEntry.getIdentifier());
        }

        /* Return the document IDs */
        return docIds;
    }

    @Override
    public List<SolrDoc> mergeIndexedDocuments(List<SolrDoc> docs) {
        List<SolrDoc> mergedDocuments = new ArrayList<SolrDoc>();

        for (ResourceEntry resourceEntry : this.resourceMap.values()) {
            for (SolrDoc doc : docs) {
                if (doc.getIdentifier().equals(resourceEntry.getIdentifier())) {
                    mergedDocuments.add(_mergeMappedReference(resourceEntry, doc));
                    break;
                }
            }
        }

        return mergedDocuments;
    }

    /*@Override
    public String getIdentifierFromResource(String resourceURI) 
    {
    	throw new NotImplementedException();
    	
    	// TODO Auto-generated method stub
    	//return null;
    }*/

    @Override
    public String getIdentifier() {
        return this.identifier;
    }

    void setIdentifier(String identifier) {
        this.identifier = identifier;
    }

    public void setIndexVisibilityDeledate(IndexVisibilityDelegate ivd) {
        this.indexVisibilityDelegate = ivd;
    }

    public static void main(String[] args) throws OREException, URISyntaxException,
            OREParserException, IOException {
        String doc = "<rdf:RDF xmlns:rdf=\"http://www.w3.org/1999/02/22-rdf-syntax-ns#\"\n"
                + "xmlns:foaf=\"http://xmlns.com/foaf/0.1/\"\n"
                + "xmlns:owl=\"http://www.w3.org/2002/07/owl#\"\n"
                + "xmlns:dc=\"http://purl.org/dc/elements/1.1/\"\n"
                + "xmlns:ore=\"http://www.openarchives.org/ore/terms/\"\n"
                + "xmlns:dcterms=\"http://purl.org/dc/terms/\"\n"
                + "xmlns:j.0=\"http://purl.org/spar/cito/\">\n"
                + "<rdf:Description rdf:about=\"https://cn-dev.test.dataone.org/cn/v1/resolve/r_test3.package.2012120601491354790946#aggregation\">\n"
                + "<rdf:type rdf:resource=\"http://www.openarchives.org/ore/terms/Aggregation\"/>\n"
                + "<ore:isDescribedBy rdf:resource=\"https://cn-dev.test.dataone.org/cn/v1/resolve/r_test3.package.2012120601491354790946\"/>\n"
                + "<dc:title>DataONE Aggregation</dc:title>\n"
                + "<ore:aggregates rdf:resource=\"https://cn-dev.test.dataone.org/cn/v1/resolve/r_test3.scimeta.2012120601491354790946\"/>\n"
                + "<ore:aggregates rdf:resource=\"https://cn-dev.test.dataone.org/cn/v1/resolve/r_test3.scidata.1.2012120601491354790946\"/>\n"
                + "<ore:aggregates rdf:resource=\"https://cn-dev.test.dataone.org/cn/v1/resolve/r_test3.scidata.2.2012120601491354790946\"/>\n"
                + "</rdf:Description>"
                + "<rdf:Description rdf:about=\"https://cn-dev.test.dataone.org/cn/v1/resolve/r_test3.scimeta.2012120601491354790946\">\n"
                + "<ore:isAggregatedBy>https://cn-dev.test.dataone.org/cn/v1/resolve/r_test3.package.2012120601491354790946#aggregation</ore:isAggregatedBy>\n"
                + "<dcterms:identifier rdf:datatype=\"http://www.w3.org/2001/XMLSchema#string\">r_test3.scimeta.2012120601491354790946</dcterms:identifier>\n"
                + "<j.0:documents rdf:resource=\"https://cn-dev.test.dataone.org/cn/v1/resolve/r_test3.scidata.1.2012120601491354790946\"/>\n"
                + "<j.0:documents rdf:resource=\"https://cn-dev.test.dataone.org/cn/v1/resolve/r_test3.scidata.2.2012120601491354790946\"/>\n"
                + "</rdf:Description>\n"
                + "<rdf:Description rdf:nodeID=\"A0\">\n"
                + " <foaf:name rdf:datatype=\"http://www.w3.org/2001/XMLSchema#string\">Java libclient</foaf:name>\n"
                + " <rdf:type rdf:resource=\"http://purl.org/dc/terms/Agent\"/>\n"
                + "</rdf:Description>\n"
                + "<rdf:Description rdf:about=\"https://cn-dev.test.dataone.org/cn/v1/resolve/r_test3.scidata.2.2012120601491354790946\">\n"
                + "  <ore:isAggregatedBy>https://cn-dev.test.dataone.org/cn/v1/resolve/r_test3.package.2012120601491354790946#aggregation</ore:isAggregatedBy>\n"
                + "  <dcterms:identifier rdf:datatype=\"http://www.w3.org/2001/XMLSchema#string\">r_test3.scidata.2.2012120601491354790946</dcterms:identifier>\n"
                + "  <j.0:isDocumentedBy rdf:resource=\"https://cn-dev.test.dataone.org/cn/v1/resolve/r_test3.scimeta.2012120601491354790946\"/>\n"
                + "</rdf:Description>\n"
                + "<rdf:Description rdf:about=\"https://cn-dev.test.dataone.org/cn/v1/resolve/r_test3.package.2012120601491354790946\">\n"
                + " <dcterms:modified rdf:datatype=\"http://www.w3.org/2001/XMLSchema#date\">2012-12-06T01:49:06-0900</dcterms:modified>\n"
                + " <ore:describes rdf:resource=\"https://cn-dev.test.dataone.org/cn/v1/resolve/r_test3.package.2012120601491354790946#aggregation\"/>\n"
                + " <rdf:type rdf:resource=\"http://www.openarchives.org/ore/terms/ResourceMap\"/>\n"
                + " <dc:creator rdf:nodeID=\"A0\"/>\n"
                + " <dcterms:identifier rdf:datatype=\"http://www.w3.org/2001/XMLSchema#string\">r_test3.package.2012120601491354790946</dcterms:identifier>\n"
                + "</rdf:Description>\n"
                + "<rdf:Description rdf:about=\"https://cn-dev.test.dataone.org/cn/v1/resolve/r_test3.scidata.1.2012120601491354790946\">\n"
                + "  <ore:isAggregatedBy>https://cn-dev.test.dataone.org/cn/v1/resolve/r_test3.package.2012120601491354790946#aggregation</ore:isAggregatedBy>\n"
                + "  <dcterms:identifier rdf:datatype=\"http://www.w3.org/2001/XMLSchema#string\">r_test3.scidata.1.2012120601491354790946</dcterms:identifier>\n"
                + "  <j.0:isDocumentedBy rdf:resource=\"https://cn-dev.test.dataone.org/cn/v1/resolve/r_test3.scimeta.2012120601491354790946\"/>\n"
                + "</rdf:Description>\n" + "</rdf:RDF>";

        ResourceMap resourceMap = new ForesiteResourceMap(doc);
        //System.out.printf("Identifier = %s\n", resourceMap.getIdentifier());
    }
}
