package org.dataone.cn.indexer;

import java.io.IOException;
import java.io.StringReader;
import java.util.Collection;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import junit.framework.TestCase;

import org.dataone.cn.indexer.convert.MemberNodeServiceRegistrationType;
import org.dataone.cn.indexer.parser.utility.ServiceTypesParser;
import org.w3c.dom.Document;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

public class ServiceTypesParserTest extends TestCase {

    // TODO move to resources?
    private static final String TEST_SERVICES_STR = 
            "<serviceTypes>" + "\n"
            + "<serviceType>" + "\n"
            + "    <name>OPeNDAP</name>" + "\n"
            + "    <matches>.*OP[Ee]NDAP.*</matches>" + "\n"
            + "</serviceType>" + "\n"
            + "<serviceType>" + "\n"
            + "    <name>WMS</name>" + "\n"
            + "    <matches>.*WMS.*</matches>" + "\n"
            + "    <matches>.*wms.*</matches>" + "\n"
            + "</serviceType>" + "\n"
            + "</serviceTypes>";    
    
    public void testServiceTypes() {
        
        DocumentBuilder db = null;
        try {
            db = DocumentBuilderFactory.newInstance().newDocumentBuilder();
        } catch (ParserConfigurationException e) {
            throw new AssertionError("Unable to create DocumentBuilder.", e);
        }
        
        InputSource is = new InputSource();
        is.setCharacterStream(new StringReader(TEST_SERVICES_STR));

        Document doc = null;
        try {
             doc = db.parse(is);
        } catch (SAXException | IOException e) {
            throw new AssertionError("Unable to create service types Document.", e);
        }
        
        Collection<MemberNodeServiceRegistrationType> serviceTypes = ServiceTypesParser.parseServiceTypes(doc);
        
        assertTrue("Service types document should contain 2 service types.", serviceTypes.size() == 2);
        MemberNodeServiceRegistrationType serviceTypesArr[] = new MemberNodeServiceRegistrationType[2];
        serviceTypes.toArray(serviceTypesArr);
        
        MemberNodeServiceRegistrationType opendapType = serviceTypesArr[0];
        MemberNodeServiceRegistrationType wmsType = serviceTypesArr[1];
        
        assertTrue("Service types document should contain an OPeNDAP service", opendapType.getName().equals("OPeNDAP"));
        assertTrue("Service types document should contain a WMS service", wmsType.getName().equals("WMS"));
        
        Collection<String> opendapPatterns = opendapType.getMatchingPatterns();
        assertTrue("OPeNDAP service type should contain match pattern: .*OP[Ee]NDAP.*", opendapPatterns.contains(".*OP[Ee]NDAP.*"));
        
        Collection<String> wmsPatterns = wmsType.getMatchingPatterns();
        assertTrue("WMS service type should contain match pattern: .*WMS.*", wmsPatterns.contains(".*WMS.*"));
        assertTrue("WMS service type should contain match pattern: .*wms.*", wmsPatterns.contains(".*wms.*"));
        
    }
}
