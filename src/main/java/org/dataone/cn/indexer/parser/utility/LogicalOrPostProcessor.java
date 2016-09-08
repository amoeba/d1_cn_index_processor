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

package org.dataone.cn.indexer.parser.utility;

import org.apache.commons.lang3.StringUtils;

public class LogicalOrPostProcessor {

    private static final String OR = "||";

    public LogicalOrPostProcessor() {

    }

    public String process(String value) {
        if (value.contains(OR)) {
            String[] partition = StringUtils.split(value, OR);
            if (partition.length > 2) {
                // only handling a single logical or at this time.
                return value;
            } else if (partition.length > 0 && !partition[0].isEmpty()) {
                return partition[0];
            } else if (partition.length > 1 && !partition[1].isEmpty()) {
                return partition[1];
            } else {
                return "";
            }
        } else {
            return value;
        }
    }
}
