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

package org.dataone.cn.indexer;

import java.util.ArrayList;

import javax.xml.parsers.ParserConfigurationException;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.dataone.cn.indexer.parser.SolrFieldResourceMap;

/**
 * SolrFieldResourceMap Tester.
 * 
 * @author <Authors name>
 * @since <pre>
 * 08 / 22 / 2011
 * </pre>
 * @version 1.0
 */

public class SolrFieldResourceMapTest extends TestCase {
    public SolrFieldResourceMapTest(String name) {
        super(name);
    }

    public void setUp() throws Exception {
        super.setUp();
    }

    public void tearDown() throws Exception {
        super.tearDown();
    }

    public static Test suite() {
        return new TestSuite(SolrFieldResourceMapTest.class);
    }

    public void testSolrFieldResourceMap() throws ParserConfigurationException {

        // <bean class="org.dataone.cn.indexer.parser.SolrFieldResourceMap">
        // <constructor-arg name="name" value="resourcemap"/>
        // <constructor-arg name="xpath"
        // value="/d063:systemMetadata/objectFormat/fmtid/text()"/>
        // <constructor-arg name="resourceMapXpath" value=""/>
        // <constructor-arg name="multivalue" value="false"/>
        // <constructor-arg name="xmlNamespaceConfig"
        // ref="xmlNamespaceResource"/>
        // <property name="resourceValueMatch"
        // value="http://www.openarchives.org/ore/terms"/>
        // </bean>

        String xpath1 = "/d063:systemMetadata/objectFormat/fmtid/text()";
        String resourceMapXPath = "/d063:systemMetadata/objectFormat/fmtid/text()";
        ArrayList<XMLNamespace> namespaces = new ArrayList<XMLNamespace>();
        namespaces.add(new XMLNamespace("cito", "http://purl.org/spar/cito/"));
        namespaces.add(new XMLNamespace("dc", "http://purl.org/dc/elements/1.1/"));
        namespaces.add(new XMLNamespace("dcterms", "http://purl.org/dc/terms/"));
        namespaces.add(new XMLNamespace("foaf", "http://xmlns.com/foaf/0.1/"));
        namespaces.add(new XMLNamespace("ore", "http://www.openarchives.org/ore/terms/"));
        namespaces.add(new XMLNamespace("rdf", "http://www.w3.org/1999/02/22-rdf-syntax-ns#"));
        namespaces.add(new XMLNamespace("rdfs1", "http://www.w3.org/2001/01/rdf-schema#"));

        XMLNamespaceConfig config = new XMLNamespaceConfig(namespaces);
        SolrFieldResourceMap solrFieldResourceMap = new SolrFieldResourceMap("resourcemap", xpath1,
                resourceMapXPath, false, config);

    }
}
