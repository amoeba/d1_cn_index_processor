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

package org.dataone.cn.index;

import java.util.HashMap;

import org.dataone.cn.indexer.convert.SolrDateConverter;
import org.dataone.cn.indexer.parser.ScienceMetadataDocumentSubprocessor;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.Resource;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "test-context.xml" })
public class SolrFieldDublinCoreTest extends BaseSolrFieldXPathTest {

    @Autowired
    private Resource qdc_ipumsi_6_3_pt_1981_SciMeta;

    @Autowired
    private Resource qdc_ipumsi_6_3_pt_1981_SysMeta;

    @Autowired
    private ScienceMetadataDocumentSubprocessor qualifiedDublicCoreSubprocessor;

    private SolrDateConverter dateConverter = new SolrDateConverter();

    private HashMap<String, String> qdcExpected = new HashMap<String, String>();

    @Before
    public void setUp() throws Exception {
        // science metadata
        qdcExpected
                .put("abstract",
                        "IPUMS-International is an effort to inventory, preserve, harmonize, and disseminate census microdata from around the world. The project has collected the world's largest archive of publicly available census samples. The data are coded and documented consistently across countries and over time to facillitate comparative research. IPUMS-International makes these data available to qualified researchers free of charge through a web dissemination system. The IPUMS project is a collaboration of the Minnesota Population Center, National Statistical Offices, and international data archives. Major funding is provided by the U.S. National Science Foundation and the Demographic and Behavioral Sciences Branch of the National Institute of Child Health and Human Development. Additional support is provided by the University of Minnesota Office of the Vice President for Research, the Minnesota Population Center, and Sun Microsystems. Detailed metadata will be found in ipumsi_6.3_pt_1981_ddic.html within the Data Package. The related metadata describes the content of the extraction of the specified sample from the IPUMS International on-line extraction system.");
        qdcExpected
                .put("keywords",
                        "Census##Technical Variables -- HOUSEHOLD##Group Quarters Variables -- HOUSEHOLD##Geography Variables -- HOUSEHOLD##Economic Variables -- HOUSEHOLD##Utilities Variables -- HOUSEHOLD##Dwelling Characteristics Variables -- HOUSEHOLD##Constructed Household Variables -- HOUSEHOLD##Technical Variables -- PERSON##Constructed Family Interrelationship Variables -- PERSON##Demographic Variables -- PERSON##Nativity and Birthplace Variables -- PERSON##Ethnicity and Language Variables -- PERSON##Education Variables -- PERSON##Work Variables -- PERSON##Income Variables -- PERSON##Migration Variables -- PERSON##Disability Variables -- PERSON##Work: Industry Variables -- PERSON##Work: Occupation Variables -- PERSON");
        qdcExpected.put("title", "IPUMS-International: Portugal 1981 Census");

        qdcExpected.put("northBoundCoord", "32.6375##42.150673##-6.190455##-31.289027");
        qdcExpected.put("geohash_1",
                "e##ew##ewd##ewds##ewdst##ewdstx##ewdstx6##ewdstx6h##ewdstx6hk");
        qdcExpected.put("site", "Portugal");

        qdcExpected.put("beginDate", dateConverter.convert("1981"));
        qdcExpected.put("endDate", dateConverter.convert("1981"));
        qdcExpected.put("pubDate", "2014-08-22T06:00:00.000Z");
        qdcExpected.put("author", "Minnesota Population Center");
        qdcExpected.put("authorSurName", "Minnesota Population Center");
        qdcExpected.put("authorSurNameSort", "Minnesota Population Center");
        qdcExpected.put("investigator", "Minnesota Population Center");
        qdcExpected.put("contactOrganization", "Minnesota Population Center");
        qdcExpected.put("origin", "Minnesota Population Center");

        qdcExpected.put("fileID", "https://" + hostname
                + "/cn/v1/resolve/ipumsi_6-3_pt_1981.dc.xml");
        qdcExpected
                .put("text",
                        "package  Minnesota Population Center  2014-08-22  2014-07-01  IPUMS-International: Portugal 1981 Census  ipumsi_6.3_pt_1981_DC.xml  IPUMS-International is an effort to inventory, preserve, harmonize, and disseminate census microdata from around the world. The project has collected the world's largest archive of publicly available census samples. The data are coded and documented consistently across countries and over time to facillitate comparative research. IPUMS-International makes these data available to qualified researchers free of charge through a web dissemination system. The IPUMS project is a collaboration of the Minnesota Population Center, National Statistical Offices, and international data archives. Major funding is provided by the U.S. National Science Foundation and the Demographic and Behavioral Sciences Branch of the National Institute of Child Health and Human Development. Additional support is provided by the University of Minnesota Office of the Vice President for Research, the Minnesota Population Center, and Sun Microsystems. Detailed metadata will be found in ipumsi_6.3_pt_1981_ddic.html within the Data Package. The related metadata describes the content of the extraction of the specified sample from the IPUMS International on-line extraction system.  Record type; Country; Year; IPUMS sample identifier; Household serial number; Number of person records in the household; Household weight; Subsample number; Donated household; Group quarters status; Number of unrelated persons; Urban-rural status; Continent and region of country; 1st subnational geographic level, world [consistent boundaries over time]; NUTS1 Region, Europe; NUTS2 Region, Europe; NUTS3 Region, Europe; Subregion, Portugal [Level 1; consistent boundaries over time]; Region, Portugal; City, Portugal; Ownership of dwelling [general version]; Ownership of dwelling [detailed version]; Electricity; Water supply; Sewage; Number of rooms; Kitchen or cooking facilities; Toilet; Bathing facilities; Year structure was built; Age of structure, coded from intervals; Household classification; Number of families in household; Number of married couples in household; Number of mothers in household; Number of fathers in household; Head's location in household; Dwelling number; Household number (within dwelling); Number of households in dwelling; Number of persons in dwelling; Number of persons in household; Dwelling created by splitting apart a large dwelling or household; Number of persons in large dwelling before it was split; Number of persons in large household before it was split; Donated dwelling; Donation strata: strata number; Geography: NUTS 1-digit; Geography: NUTS 2-digit; Geography: NUTS 3-digit [modified]; City; Size of place of residence; Type of living quarter; Dwelling type: occupancy status; Occupancy status; Electricity; Water supply system; Toilet facilities; Bath or shower; Sewage disposal system; Kitchen; Number of rooms in the dwelling; Mortgage or loan resulting from the purchase of this dwelling; Housing unit ownership; Occupancy; Period of construction; Type of roof; Household type; Record type [person version]; Country [person version]; Year [person version]; IPUMS sample identifier [person version]; Household serial number [person version]; Person number; Person weight; Mother's location in household; Father's location in household; Spouse's location in household; Rule for linking parent; Rule for linking spouse; Probable stepmother; Probable stepfather; Man with more than one wife linked; Woman is second or higher order wife; Family unit membership; Number of own family members in household; Number of own children in household; Number of own children under age 5 in household; Age of eldest own child in household; Age of youngest own child in household; Relationship to household head [general version]; Relationship to household head [detailed version]; Relationship to head, Europe; Age; Age, grouped into intervals; Sex; Marital status [general version]; Marital status [detailed version]; Marital status, Europe; Nativity status; Country of birth; Subregion of birth, Portugal; Region of birth, Europe, NUTS1; Region of birth, Europe, NUTS2; Region of birth, Europe, NUTS3; Citizenship; Country of citizenship; Religion [general version]; Religion [detailed version]; School attendance; Literacy; Educational attainment, international recode [general version]; Educational attainment, international recode [detailed version]; Educational attainment, Portugal; Educational attainment, Europe; Employment status [general version]; Employment status [detailed version]; Employment status, Europe; Occupation, ISCO general; Occupation, unrecoded; Industry, general recode; Industry, unrecoded; Class of worker [general version]; Class of worker [detailed version]; Class of worker, Europe; Source of livelihood; Migration status, 1 year; Subregion of residence 1 year ago, Portugal; Subregion of residence 7 years ago, Portugal; Employment disability; Person number (within household); Relationship to household head; Age; Sex; Marital status; Family type; Religion; EU subregion of usual residence on December 31, 1973: NUTS 3-digit [modified]; Place of usual residence on December 31, 1973; EU subregion of usual residence on December 31, 1979: NUTS 3-digit [modified]; Place of usual residence on December 31, 1979; Place of birth: NUTS 3-digits [modified]; Place of birth: Portugal or foreign country; Place of birth: country; Citizenship; Country of citizenship; Literacy; School attendance; Educational attainment; Course of study for professional, post-secondary, or tertiary degree; Employment status; Professional situation; Sector of economic activity; Industry; Main occupation; Source of livelihood; Hours worked in a specific profession during the indicated week; Place of work or study: Same or different municipality; Place of work or study: Geographic location; Main mode of transport; Sequence number of the individual inside of the household; Person number of the spouse; Person number of the father; Person number of the mother;  Census  Technical Variables -- HOUSEHOLD  Group Quarters Variables -- HOUSEHOLD  Geography Variables -- HOUSEHOLD  Economic Variables -- HOUSEHOLD  Utilities Variables -- HOUSEHOLD  Dwelling Characteristics Variables -- HOUSEHOLD  Constructed Household Variables -- HOUSEHOLD  Technical Variables -- PERSON  Constructed Family Interrelationship Variables -- PERSON  Demographic Variables -- PERSON  Nativity and Birthplace Variables -- PERSON  Ethnicity and Language Variables -- PERSON  Education Variables -- PERSON  Work Variables -- PERSON  Income Variables -- PERSON  Migration Variables -- PERSON  Disability Variables -- PERSON  Work: Industry Variables -- PERSON  Work: Occupation Variables -- PERSON  Portugal  northlimit=32.637500; eastlimit=-6.190455; southlimit=42.150673; westlimit=-31.289027; name=Portugal;  1981  http://international.ipums.org  ipumsi_6.3_pt_1981_ddic.xml  ipumsi_6.3_pt_1981_ddic_xml.htm  IPUMS-International distributes integrated microdata of individuals and households only by agreement of collaborating national statistical offices and under the strictest of confidence. Before data may be distributed to an individual researcher, an electronic license agreement must be signed and approved. (see http://international.ipums.org) ipumsi_6-3_pt_1981.dc.xml");

        // system metadata
        qdcExpected.put("id", "ipumsi_6-3_pt_1981.dc.xml");
        qdcExpected.put("formatId",
                "http://dublincore.org/schemas/xmls/qdc/2008/02/11/qualifieddc.xsd");
        qdcExpected.put("formatType", "METADATA");
        qdcExpected.put("size", "8734");
        qdcExpected.put("checksum", "f5975f877816caea2f2be2e2f6b7ddb5");
        qdcExpected.put("checksumAlgorithm", "MD5");
        qdcExpected.put("submitter", "CN=urn:node:mnTestMPC,DC=dataone,DC=org");
        qdcExpected.put("rightsHolder",
                "CN=Judy Kallestad A13391,O=University of Minnesota,C=US,DC=cilogon,DC=org");
        qdcExpected.put("replicationAllowed", "");
        qdcExpected.put("numberReplicas", "");
        qdcExpected.put("preferredReplicationMN", "");
        qdcExpected.put("blockedReplicationMN", "");
        qdcExpected.put("obsoletes", "");
        qdcExpected.put("obsoletedBy", "");
        qdcExpected.put("dateUploaded", dateConverter.convert("2014-08-28T20:55:19.003582"));
        qdcExpected.put("dateModified", dateConverter.convert("2014-08-28T20:55:19.034555Z"));
        qdcExpected.put("datasource", "urn:node:mnTestMPC");
        qdcExpected.put("authoritativeMN", "urn:node:mnTestMPC");
        qdcExpected.put("replicaMN", "");
        qdcExpected.put("replicaVerifiedDate", "");
        qdcExpected.put("readPermission", "public");
        qdcExpected.put("writePermission", "");
        qdcExpected.put("changePermission", "");
        qdcExpected.put("isPublic", "true");
        qdcExpected.put("dataUrl", "https://" + hostname
                + "/cn/v1/resolve/ipumsi_6-3_pt_1981.dc.xml");
    }

    @Test
    public void testQualifiedDCFieldParsing() throws Exception {
        testXPathParsing(qualifiedDublicCoreSubprocessor, qdc_ipumsi_6_3_pt_1981_SysMeta,
                qdc_ipumsi_6_3_pt_1981_SciMeta, qdcExpected, "ipumsi_6-3_pt_1981.dc.xml");
    }

}
