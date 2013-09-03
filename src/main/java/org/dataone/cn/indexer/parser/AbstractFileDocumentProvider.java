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

import java.io.File;
import java.io.IOException;

import javax.xml.parsers.ParserConfigurationException;

import org.dataone.cn.indexer.XPathDocumentParser;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

/**
 * Document Provider designed to load documents based off of file name and predefined directory.
 * Do not believe this class is actually used by index processing. -- slr on 8-30-13
 * 
 * Created by IntelliJ IDEA.
 * User: Porter
 * Date: 9/28/11
 * Time: 12:33 PM
 */
public class AbstractFileDocumentProvider implements IDocumentProvider {

    private String baseDirectory = null;

    public Document GetDocument(String identifier) throws ParserConfigurationException,
            IOException, SAXException {
        File f = new File(baseDirectory, identifier + ".xml");
        return GetDocumentFromFile(f);
    }

    public Document GetDocumentFromFile(File f) throws ParserConfigurationException, IOException,
            SAXException {
        Document xmlFile = parseDocument(f);
        return xmlFile;
    }

    public String getBaseDirectory() {
        return baseDirectory;
    }

    public void setBaseDirectory(String baseDirectory) {
        this.baseDirectory = baseDirectory;
    }

    private Document parseDocument(File f) {
        try {
            Document doc = XPathDocumentParser.getDocumentBuilder().parse(f);
            return doc;

        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
}
