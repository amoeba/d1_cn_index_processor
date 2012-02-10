package org.dataone.cn.indexer.solrhttp;

import java.io.CharArrayWriter;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringEscapeUtils;

/**
 * User: Porter Date: 7/25/11 Time: 4:14 PM Contains constants FILED_XXXX for
 * DataOne system metdata solr index fields.
 * 
 * @see SolrElementAdd
 */
public class SolrElementField {

    public static final String FIELD_ID = "id";
    public static final String FIELD_OBJECTFORMAT = "objectformat";
    public static final String FIELD_SIZE = "size";
    public static final String FIELD_CHECKSUM = "checksum";
    public static final String FIELD_CHECKSUMALGORITHM = "checksumAlgorithm";
    public static final String FIELD_SUBMITTER = "submitter";
    public static final String FIELD_RIGHTSHOLDER = "rightsholder";
    public static final String FIELD_REP_ALLOWED = "rep_allowed";
    public static final String FIELD_N_REPLICAS = "n_replicas";
    public static final String FIELD_PREF_REP_MN = "pref_rep_mn";
    public static final String FIELD_BLOCKED_REP_MN = "blocked_rep_mn";
    public static final String FIELD_OBSOLETES = "obsoletes";
    public static final String FIELD_DATEUPLOADED = "dateuploaded";
    public static final String FIELD_DATEMODIFIED = "datemodified";
    public static final String FIELD_ORIGIN_MN = "datasource";
    public static final String FIELD_AUTH_MN = "auth_mn";
    public static final String FIELD_REPLICA_MN = "replica_mn";
    public static final String FIELD_RESOURCEMAP = "resourcemap";
    public static final String FIELD_DOCUMENTS = "documents";
    public static final String FIELD_ISDOCUMENTEDBY = "isDocumentedBy";
    public static final String FIELD_READPERMISSION = "readPermission";
    public static final String FIELD_WRITEPERMISSION = "writePermission";
    public static final String FIELD_EXECUTEPERMISSION = "executePermission";
    public static final String FIELD_CHANGEPERMISSION = "changePermission";
    public static final String FIELD_ISPUBLIC = "isPublic";
    public static final String FIELD_DECADE = "decade";
    public static final String FIELD_BEGIN_DATE = "beginDate";
    public static final String FIELD_END_DATE = "endDate";

    public static final char[] ELEMENT_FIELD_OPEN = "<field ".toCharArray();
    public static final char[] ELEMENT_FIELD_CLOSE = "</field>".toCharArray();
    public static final String ATTRIBUTE_NAME = "name";

    private String name = null;
    private String value = null;
    private boolean escapeXML = true;

    public SolrElementField() {
    }

    public SolrElementField(String name, String value) {
        this.name = name;
        this.value = value;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public void serialize(OutputStream outputStream, String encoding) throws IOException {
        if (value == null || value.equals("")) {
            return;
        }

        IOUtils.write(ELEMENT_FIELD_OPEN, outputStream, encoding);

        java.io.CharArrayWriter cw = new CharArrayWriter();

        cw.append(ATTRIBUTE_NAME);
        cw.append("=\"");
        cw.append(name);
        cw.append("\">");

        IOUtils.write(cw.toCharArray(), outputStream, encoding);
        char[] toWrite = null;
        if (escapeXML) {
            toWrite = StringEscapeUtils.escapeXml(value).toCharArray();
        } else {
            toWrite = value.toCharArray();
        }
        IOUtils.write(toWrite, outputStream, encoding);

        IOUtils.write(ELEMENT_FIELD_CLOSE, outputStream, encoding);

        outputStream.flush();
    }

    public boolean isEscapeXML() {
        return escapeXML;
    }

    public void setEscapeXML(boolean escapeXML) {
        this.escapeXML = escapeXML;
    }
}
