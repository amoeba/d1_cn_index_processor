package org.dataone.cn.indexer.convert;

import java.util.Collection;

import junit.framework.TestCase;

import org.dataone.cn.indexer.convert.MemberNodeServiceRegistrationType;
import org.dataone.cn.indexer.convert.MemberNodeServiceRegistrationTypeDocumentService;
import org.dataone.cn.indexer.parser.utility.ServiceTypesParser;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.w3c.dom.Document;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "../../index/test-context.xml" })
public class ServiceTypesParserTest extends TestCase {

    @Autowired
    MemberNodeServiceRegistrationTypeDocumentService serviceTypeDocService;
    
    @Test
    public void testServiceTypes() {

        Document doc = serviceTypeDocService.getMemberNodeServiceRegistrationTypeDocument();
        assertTrue("Fetched services Document shouldn't be null.", doc != null);
        
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
