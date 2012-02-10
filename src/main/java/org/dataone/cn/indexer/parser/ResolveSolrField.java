package org.dataone.cn.indexer.parser;

import java.net.InetAddress;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;

import javax.xml.xpath.XPath;

import org.dataone.cn.indexer.solrhttp.SolrElementField;
import org.w3c.dom.Document;

public class ResolveSolrField extends SolrField {
    private static final String RESOLVE_PATH = "/cn/v1/resolve/";

    public ResolveSolrField(String name) {
        setName(name);
    }

    @Override
    public List<SolrElementField> getFields(Document doc, String identifier) throws Exception {
        List<SolrElementField> fields = new ArrayList<SolrElementField>();
        String hostname = InetAddress.getLocalHost().getCanonicalHostName();
        String pid = identifier;
        pid = URLEncoder.encode(pid, "UTF-8");
        fields.add(new SolrElementField(getName(), "https://" + hostname + RESOLVE_PATH + pid));
        return fields;
    }

    @Override
    public void initExpression(XPath xpathObject) {
        // this solr field does not make use of an xpath expression so override
        // with empty behavior.
    }
}
