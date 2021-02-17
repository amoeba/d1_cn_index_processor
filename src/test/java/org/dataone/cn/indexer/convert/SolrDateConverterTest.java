/**
 * This work was created by participants in the DataONE project, and is
 * jointly copyrighted by participating institutions in DataONE. For 
 * more information on DataONE, see our web site at http://dataone.org.
 *
 *   Copyright 2021
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
 */
package org.dataone.cn.indexer.convert;

import static org.junit.Assert.assertTrue;
import org.junit.Test;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

/**
 * 
 * @author tao
 * A junit test class to test the date converter
 * 
 */
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class SolrDateConverterTest {
    
    private SolrDateConverter dateConverter = new SolrDateConverter();
    
    /**
     * Test the convert method
     * @throws Exception
     */
    @Test
    public void testConvert() throws Exception {
        String date = "2003-04-21T09:40:00";
        String output = dateConverter.convert(date);
        System.out.println("The output string is " + output);
        assertTrue(output.equals("2003-04-21T09:40:00.000Z"));
        
        date = "2001.07.04 AD at 12:08:56 PDT";
        output = dateConverter.convert(date);
        System.out.println("The output string is " + output);
        assertTrue(output.equals(""));
        
        date = "Wed, Jul 4, '01";
        output = dateConverter.convert(date);
        System.out.println("The output string is " + output);
        assertTrue(output.equals(""));
        
        date = "12:08 PM";
        output = dateConverter.convert(date);
        System.out.println("The output string is " + output);
        assertTrue(output.equals(""));
        
        date = "02001.July.04 AD 12:08 PM";
        output = dateConverter.convert(date);
        System.out.println("The output string is " + output);
        assertTrue(output.equals(""));
        
        date = "Wed, 4 Jul 2001 12:08:56 -0700";
        output = dateConverter.convert(date);
        System.out.println("The output string is " + output);
        assertTrue(output.equals(""));
        
        date = "010704120856-0700";
        output = dateConverter.convert(date);
        System.out.println("The output string is " + output);
        assertTrue(output.equals(""));
        
        date = "2001-07-04T12:08:56.235+0700";
        output = dateConverter.convert(date);
        System.out.println("The output string is " + output);
        assertTrue(output.equals("2001-07-04T05:08:56.235Z"));
        
        date = "2001-07-04T12:08:56.235-08:00";
        output = dateConverter.convert(date);
        System.out.println("The output string is " + output);
        assertTrue(output.equals("2001-07-04T20:08:56.235Z"));
        
        date = "2001-W27-3";
        output = dateConverter.convert(date);
        System.out.println("The output string is " + output);
        assertTrue(output.equals("2001-07-04T00:00:00.000Z"));
        
        date = "2011";
        output = dateConverter.convert(date);
        System.out.println("The output string is " + output);
        assertTrue(output.equals("2011-01-01T00:00:00.000Z"));
        
    }

}
