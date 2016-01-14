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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.dataone.cn.indexer.convert.MemberNodeServiceRegistrationType;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;


/**
 * Helper class for parsing a {@link Document} of service types into 
 * service names and the corresponding regex expressions that are used 
 * to match fields in a service type doc to the proper service name.
 * 
 * @author Andrei
 */
public class MemberNodeServiceRegistrationTypesParser {

    private static XPath xPath =  null;
    static {
        xPath = XPathFactory.newInstance().newXPath();
    }
    
    /**
     * Extracts a Collection of {@link MemberNodeServiceRegistrationType}s (each containing the name and 
     * match patterns for a type of service) from a given service types {@link Document}.
     * 
     * @param serviceTypesDoc the document containing the service types and their regex match patterns
     * @return a Collection of {@link MemberNodeServiceRegistrationType}s contained in the {@link Document}
     */
    public static Collection<MemberNodeServiceRegistrationType> parseServiceTypes(Document serviceTypesDoc) {
        
        Collection<MemberNodeServiceRegistrationType> serviceTypes = new ArrayList<MemberNodeServiceRegistrationType>();
        
        String serviceNamesExpr = "/serviceTypes/serviceType/name/text()";
        NodeList nameNodes = null;
        try {
            nameNodes = (NodeList) xPath.compile(serviceNamesExpr).evaluate(serviceTypesDoc, XPathConstants.NODESET);
        } catch (XPathExpressionException e) {
            throw new AssertionError("Unable to parse service names from service types document.", e);
        }
        
        for (int i = 0; i < nameNodes.getLength(); i++) {
            Node nameNode = nameNodes.item(i);
            String serviceName = nameNode.getTextContent();
            
            String serviceRegexExpr = "/serviceTypes/serviceType[name/text()=\"" + serviceName + "\"]/matches/text()";
            NodeList regexNodes = null;
            try {
                regexNodes = (NodeList) xPath.compile(serviceRegexExpr).evaluate(serviceTypesDoc, XPathConstants.NODESET);
            } catch (XPathExpressionException e) {
                throw new AssertionError("Unable to parse service regexes from service types document for service name " + serviceName + ".", e);
            }
            
            List<String> regexes = new ArrayList<String>();
            for (int j = 0; j < regexNodes.getLength(); j++) {
                Node regexNode = regexNodes.item(j);
                regexes.add(regexNode.getTextContent());
            }
            
            MemberNodeServiceRegistrationType serviceType = new MemberNodeServiceRegistrationType();
            serviceType.setName(serviceName);
            serviceType.setMatchingPatterns(regexes);
            
            serviceTypes.add(serviceType);
        }
        
        return serviceTypes;
    }
}
