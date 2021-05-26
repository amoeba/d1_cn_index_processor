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
import static org.junit.Assert.assertFalse;
import org.junit.Test;

import org.dataone.cn.indexer.convert.GeohashConverter;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

/**
 * 
 * @author tao
 * A junit test class to test the date converter
 * 
 */
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class SolrGeohashConverterTest {
    
    public static final int GEOHASH_LEVEL_1 = 1;
    public static final int GEOHASH_LEVEL_2 = 2;
    public static final int GEOHASH_LEVEL_3 = 3;
    public static final int GEOHASH_LEVEL_4 = 4;
    public static final int GEOHASH_LEVEL_5 = 5;
    public static final int GEOHASH_LEVEL_6 = 6;
    public static final int GEOHASH_LEVEL_7 = 7;
    public static final int GEOHASH_LEVEL_8 = 8;
    public static final int GEOHASH_LEVEL_9 = 9;

    private GeohashConverter geohashConverter =  new GeohashConverter();
    
    /**
     * Test the Geohash converter 
     * <p>
     * The Geohash converter is tested by converting bounding boxes that cross the International
     * Date Line and those that don't. Th
     * Note that internally, the GeohashConverter first calculates the center point of the bounding box
     * and from that calculates the geohash for a specified level (i.e. 1-9).
     * Also note that the IDL does not have a continuous longituted at -180 (or +f180) , as it varies around This
     * longitutde, so we are really testing specifying east and west on either side of this hypothetical line.
     * </p>
     * @throws Exception
     */
    @Test
    public void testConvert() throws Exception {
        
        String latLongBox = null;
        // This bounding box doesn't cross the International Date Line
        // north, south, east, west
        latLongBox = "73.0 67.0 -51.0 -63.0";
        // Center point of bbox (lat, long): 70.0, -57.0,
        assertTrue(getGeohash(GEOHASH_LEVEL_1, latLongBox).equals("f"));
        assertTrue(getGeohash(GEOHASH_LEVEL_2, latLongBox).equals("fs"));
        assertTrue(getGeohash(GEOHASH_LEVEL_3, latLongBox).equals("fsr"));
        assertTrue(getGeohash(GEOHASH_LEVEL_4, latLongBox).equals("fsrq"));
        assertTrue(getGeohash(GEOHASH_LEVEL_5, latLongBox).equals("fsrqn"));
        assertTrue(getGeohash(GEOHASH_LEVEL_6, latLongBox).equals("fsrqnz"));
        assertTrue(getGeohash(GEOHASH_LEVEL_7, latLongBox).equals("fsrqnzj"));
        assertTrue(getGeohash(GEOHASH_LEVEL_8, latLongBox).equals("fsrqnzjg"));
        assertTrue(getGeohash(GEOHASH_LEVEL_9, latLongBox).equals("fsrqnzjge"));
        
        // Test a case that should fail.
        assertFalse(getGeohash(GEOHASH_LEVEL_9, latLongBox).equals("abcdef"));
        
        // This bounding box does cross the IDL.
        latLongBox = "75.0 50.0 -125.0 175.0";
        // Center point of bbox (lat, long  ): 62.5, -155.0
        assertTrue(getGeohash(GEOHASH_LEVEL_1, latLongBox).equals("b"));
        assertTrue(getGeohash(GEOHASH_LEVEL_2, latLongBox).equals("be"));
        assertTrue(getGeohash(GEOHASH_LEVEL_3, latLongBox).equals("be1"));
        assertTrue(getGeohash(GEOHASH_LEVEL_4, latLongBox).equals("be1g"));
        assertTrue(getGeohash(GEOHASH_LEVEL_5, latLongBox).equals("be1g8"));
        assertTrue(getGeohash(GEOHASH_LEVEL_6, latLongBox).equals("be1g8c"));
        assertTrue(getGeohash(GEOHASH_LEVEL_7, latLongBox).equals("be1g8cu"));
        assertTrue(getGeohash(GEOHASH_LEVEL_8, latLongBox).equals("be1g8cu2"));
        assertTrue(getGeohash(GEOHASH_LEVEL_9, latLongBox).equals("be1g8cu2y"));
        
        latLongBox = "73 64 -150 -170";
        // Center point of bbox (lat, long  ): 68.5, -160.0
        assertTrue(getGeohash(GEOHASH_LEVEL_1, latLongBox).equals("b"));
        assertTrue(getGeohash(GEOHASH_LEVEL_2, latLongBox).equals("bk"));
        assertTrue(getGeohash(GEOHASH_LEVEL_3, latLongBox).equals("bkn"));
        assertTrue(getGeohash(GEOHASH_LEVEL_4, latLongBox).equals("bknj"));
        assertTrue(getGeohash(GEOHASH_LEVEL_5, latLongBox).equals("bknjx"));
        assertTrue(getGeohash(GEOHASH_LEVEL_6, latLongBox).equals("bknjxn"));
        assertTrue(getGeohash(GEOHASH_LEVEL_7, latLongBox).equals("bknjxn5"));
        assertTrue(getGeohash(GEOHASH_LEVEL_8, latLongBox).equals("bknjxn59"));
        assertTrue(getGeohash(GEOHASH_LEVEL_9, latLongBox).equals("bknjxn593"));
        
        latLongBox = "-40 -50 -160 -170";
        // Center point of bbox (lat, long  ): -45.0, -165.0, 2248j248j
        assertTrue(getGeohash(GEOHASH_LEVEL_1, latLongBox).equals("2"));
        assertTrue(getGeohash(GEOHASH_LEVEL_2, latLongBox).equals("22"));
        assertTrue(getGeohash(GEOHASH_LEVEL_3, latLongBox).equals("224"));
        assertTrue(getGeohash(GEOHASH_LEVEL_4, latLongBox).equals("2248"));
        assertTrue(getGeohash(GEOHASH_LEVEL_5, latLongBox).equals("2248j"));
        assertTrue(getGeohash(GEOHASH_LEVEL_6, latLongBox).equals("2248j2"));
        assertTrue(getGeohash(GEOHASH_LEVEL_7, latLongBox).equals("2248j24"));
        assertTrue(getGeohash(GEOHASH_LEVEL_8, latLongBox).equals("2248j248"));
        assertTrue(getGeohash(GEOHASH_LEVEL_9, latLongBox).equals("2248j248j"));
        
        latLongBox = "63.0 61.0 15.5 15.3";
        // Center point of bbox (lat, long  ): 62.0, 15.4, u74bwqvbn 
        assertTrue(getGeohash(GEOHASH_LEVEL_1, latLongBox).equals("u"));
        assertTrue(getGeohash(GEOHASH_LEVEL_2, latLongBox).equals("u7"));
        assertTrue(getGeohash(GEOHASH_LEVEL_3, latLongBox).equals("u74"));
        assertTrue(getGeohash(GEOHASH_LEVEL_4, latLongBox).equals("u74b"));
        assertTrue(getGeohash(GEOHASH_LEVEL_5, latLongBox).equals("u74bw"));
        assertTrue(getGeohash(GEOHASH_LEVEL_6, latLongBox).equals("u74bwq"));
        assertTrue(getGeohash(GEOHASH_LEVEL_7, latLongBox).equals("u74bwqv"));
        assertTrue(getGeohash(GEOHASH_LEVEL_8, latLongBox).equals("u74bwqvb"));
        assertTrue(getGeohash(GEOHASH_LEVEL_9, latLongBox).equals("u74bwqvbn"));
        
        latLongBox = "10.0 -10.0 -175.0 175.0";
        // Center point of bbox (lat, long  ): 0.0, 180.0, xbpbpbpbp
        assertTrue(getGeohash(GEOHASH_LEVEL_1, latLongBox).equals("x"));
        assertTrue(getGeohash(GEOHASH_LEVEL_2, latLongBox).equals("xb"));
        assertTrue(getGeohash(GEOHASH_LEVEL_3, latLongBox).equals("xbp"));
        assertTrue(getGeohash(GEOHASH_LEVEL_4, latLongBox).equals("xbpb"));
        assertTrue(getGeohash(GEOHASH_LEVEL_5, latLongBox).equals("xbpbp"));
        assertTrue(getGeohash(GEOHASH_LEVEL_6, latLongBox).equals("xbpbpb"));
        assertTrue(getGeohash(GEOHASH_LEVEL_7, latLongBox).equals("xbpbpbp"));
        assertTrue(getGeohash(GEOHASH_LEVEL_8, latLongBox).equals("xbpbpbpb"));
        assertTrue(getGeohash(GEOHASH_LEVEL_9, latLongBox).equals("xbpbpbpbp"));
    
    }    
    
    public String getGeohash(int level, String latLongVal) {

        geohashConverter.setLength(level);
        String geohashValue = geohashConverter.convert(latLongVal);
        return geohashValue;

    }
}
