package org.dataone.cn.indexer.convert;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "../../index/test-context.xml" })
public class MemberNodeServiceRegistrationTypeConverterTest {

//    @Autowired
//    MemberNodeServiceRegistrationTypeConverter serviceTypesConverter;
    MemberNodeServiceRegistrationTypeConverter serviceTypesConverter = new MemberNodeServiceRegistrationTypeConverter();
    
    @Test
    public void testConvertWMS() {
        assertTrue("\"WMS\" should be converted to service type WMS.", 
                "WMS".equals(serviceTypesConverter.convert("WMS")));
        assertTrue("\"Wms\" should be converted to service type WMS.", 
                "WMS".equals(serviceTypesConverter.convert("Wms")));
        assertTrue("\"wms\" should be converted to service type WMS.", 
                "WMS".equals(serviceTypesConverter.convert("wms")));
        
        assertFalse("\"W-M-S\" should not be converted to service type WMS.", 
                "WMS".equals(serviceTypesConverter.convert("W-M-S")));
        assertFalse("\"OPeNDAP\" should not be converted to service type WMS.", 
                "WMS".equals(serviceTypesConverter.convert("OPeNDAP")));
    }
    
    @Test
    public void testConvertOPeNDAP() {
        assertTrue("\"OPeNDAP\" should be converted to service type OPeNDAP.", 
                "OPeNDAP".equals(serviceTypesConverter.convert("OPeNDAP")));
        assertTrue("\"OPENDAP\" should be converted to service type OPeNDAP.", 
                "OPeNDAP".equals(serviceTypesConverter.convert("OPENDAP")));
        assertTrue("\"opendap\" should be converted to service type OPeNDAP.", 
                "OPeNDAP".equals(serviceTypesConverter.convert("opendap")));
        
        assertFalse("\"WMS\" should not be converted to service type OPeNDAP.", 
                "OPeNDAP".equals(serviceTypesConverter.convert("WMS")));
        assertFalse("\"ERRDAP\" should not be converted to service type OPeNDAP.", 
                "OPeNDAP".equals(serviceTypesConverter.convert("ERRDAP")));
    }
    
}
