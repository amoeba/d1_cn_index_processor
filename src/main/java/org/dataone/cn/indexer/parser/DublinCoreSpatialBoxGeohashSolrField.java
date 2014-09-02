/**
 * This work was created by participants in the DataONE project, and is
 * jointly copyrighted by participating institutions in DataONE. For 
 * more information on DataONE, see our web site at http://dataone.org.
 *
 *   Copyright ${year}
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and 
 * limitations under the License.
 * 
 * $Id$
 */

package org.dataone.cn.indexer.parser;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.dataone.cn.indexer.convert.GeohashConverter;
import org.dataone.cn.indexer.parser.utility.DublinCoreSpatialBoxParsingUtility;
import org.dataone.cn.indexer.solrhttp.SolrElementField;
import org.w3c.dom.Document;

public class DublinCoreSpatialBoxGeohashSolrField extends SolrField implements ISolrField {

    private static Logger logger = Logger.getLogger(DublinCoreSpatialBoxGeohashSolrField.class
            .getName());

    static final String GEOHASH_LEVEL_1_FIELD = "geohash_1";
    private static final String GEOHASH_LEVEL_2_FIELD = "geohash_2";
    private static final String GEOHASH_LEVEL_3_FIELD = "geohash_3";
    private static final String GEOHASH_LEVEL_4_FIELD = "geohash_4";
    private static final String GEOHASH_LEVEL_5_FIELD = "geohash_5";
    private static final String GEOHASH_LEVEL_6_FIELD = "geohash_6";
    private static final String GEOHASH_LEVEL_7_FIELD = "geohash_7";
    private static final String GEOHASH_LEVEL_8_FIELD = "geohash_8";
    private static final String GEOHASH_LEVEL_9_FIELD = "geohash_9";

    private static DublinCoreSpatialBoxParsingUtility boxParsingUtility = new DublinCoreSpatialBoxParsingUtility();
    private GeohashConverter geohashConverter = new GeohashConverter();

    public DublinCoreSpatialBoxGeohashSolrField() {
    }

    public DublinCoreSpatialBoxGeohashSolrField(String xpath) {
        this.xpath = xpath;
    }

    public List<SolrElementField> getFields(Document doc, String identifier) throws Exception {
        return parseBox(doc);
    }

    private List<SolrElementField> parseBox(Document doc) {
        List<SolrElementField> fields = new ArrayList<SolrElementField>();

        String nodeValue = boxParsingUtility.extractNodeValue(doc, this.xPathExpression);

        String northValue = boxParsingUtility.extractDirectionalValue(nodeValue,
                DublinCoreSpatialBoxParsingUtility.DC_BOX_NORTH_PROPERTY);
        String southValue = boxParsingUtility.extractDirectionalValue(nodeValue,
                DublinCoreSpatialBoxParsingUtility.DC_BOX_SOUTH_PROPERTY);
        String eastValue = boxParsingUtility.extractDirectionalValue(nodeValue,
                DublinCoreSpatialBoxParsingUtility.DC_BOX_EAST_PROPERTY);
        String westValue = boxParsingUtility.extractDirectionalValue(nodeValue,
                DublinCoreSpatialBoxParsingUtility.DC_BOX_WEST_PROPERTY);

        String latLongValue = northValue + " " + southValue + " " + eastValue + " " + westValue;

        if (latLongValue != null && StringUtils.isNotEmpty(latLongValue)) {
            latLongValue = latLongValue.trim();
            addGeohashLevelField(1, GEOHASH_LEVEL_1_FIELD, latLongValue, fields);
            addGeohashLevelField(2, GEOHASH_LEVEL_2_FIELD, latLongValue, fields);
            addGeohashLevelField(3, GEOHASH_LEVEL_3_FIELD, latLongValue, fields);
            addGeohashLevelField(4, GEOHASH_LEVEL_4_FIELD, latLongValue, fields);
            addGeohashLevelField(5, GEOHASH_LEVEL_5_FIELD, latLongValue, fields);
            addGeohashLevelField(6, GEOHASH_LEVEL_6_FIELD, latLongValue, fields);
            addGeohashLevelField(7, GEOHASH_LEVEL_7_FIELD, latLongValue, fields);
            addGeohashLevelField(8, GEOHASH_LEVEL_8_FIELD, latLongValue, fields);
            addGeohashLevelField(9, GEOHASH_LEVEL_9_FIELD, latLongValue, fields);
        }

        return fields;
    }

    private void addGeohashLevelField(int level, String indexFieldName, String latLongVal,
            List<SolrElementField> fields) {

        geohashConverter.setLength(level);
        String geohasVal = geohashConverter.convert(latLongVal);

        if (StringUtils.isNotEmpty(geohasVal)) {
            fields.add(new SolrElementField(indexFieldName, geohasVal));
        }
    }

}
