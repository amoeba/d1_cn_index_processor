package org.dataone.cn.indexer.parser;

import java.util.List;

import org.apache.log4j.Logger;
import org.dataone.cn.indexer.solrhttp.SolrElementField;
import org.w3c.dom.Document;

public class DataCiteSpatialBoxGeohashSolrField extends
        DataCiteSpatialBoxBoundingCoordinatesSolrField implements ISolrField {

    private static Logger logger = Logger
            .getLogger(DataCiteSpatialBoxBoundingCoordinatesSolrField.class.getName());

    public DataCiteSpatialBoxGeohashSolrField() {
    }

    public List<SolrElementField> getFields(Document doc, String identifier) throws Exception {
        return boxParsingUtility
                .parseDataCiteGeohash(doc, boxXPathExpression, pointXPathExpression);
    }

}
