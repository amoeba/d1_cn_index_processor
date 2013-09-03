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

import java.io.IOException;

import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.xml.sax.SAXException;

/**
 * Document provider for science metadata.  
 * Defers to AbstractFileDocumentProvider to create a w3c.dom.Document object.
 * Class does not appear to be used - slr on 8-30-13.
 * 
 * Created by IntelliJ IDEA.
 * User: Porter
 * Date: 9/28/11
 * Time: 12:33 PM
 */

public class ScienceMetadataFileDocumentProvider extends AbstractFileDocumentProvider {

    @Override
    public Document GetDocument(String identifier) throws ParserConfigurationException,
            IOException, SAXException {
        return super.GetDocument(identifier);
    }
}
