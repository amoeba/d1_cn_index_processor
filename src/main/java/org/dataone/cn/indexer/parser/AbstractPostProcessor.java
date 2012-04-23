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

import org.dataone.cn.indexer.solrhttp.SolrDoc;

import java.util.List;

/**
 * User: Porter
 */
public class AbstractPostProcessor {
    protected String matchField = null;
    protected List<String> matchValue = null;

    public boolean match(SolrDoc solrDoc){
        String value = solrDoc.getFirstFieldValue(matchField);

        if(value == null){
            return false;
        }

        for (String valueToMatch : matchValue) {
            if(valueToMatch == null){
                continue;
            }
            else if(value.equals(valueToMatch)){
                return true;
            }
        }
        return false;
    }

    public String getMatchField() {
        return matchField;
    }

    public void setMatchField(String matchField) {
        this.matchField = matchField;
    }

    public List<String> getMatchValue() {
        return matchValue;
    }

    public void setMatchValue(List<String> matchValue) {
        this.matchValue = matchValue;
    }
}
