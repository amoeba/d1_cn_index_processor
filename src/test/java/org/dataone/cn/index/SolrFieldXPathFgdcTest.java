package org.dataone.cn.index;

import java.net.InetAddress;
import java.util.HashMap;

import org.dataone.cn.indexer.convert.FgdcDateConverter;
import org.dataone.cn.indexer.convert.IConverter;
import org.dataone.cn.indexer.convert.SolrDateConverter;
import org.dataone.cn.indexer.parser.ScienceMetadataDocumentSubprocessor;
import org.dataone.cn.indexer.parser.SolrField;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.Resource;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.w3c.dom.Document;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "test-context.xml" })
public class SolrFieldXPathFgdcTest extends BaseSolrFieldXPathTest {

    @Autowired
    private Resource fdgc01111999SciMeta;

    @Autowired
    private Resource fdgc01111999SysMeta;

    @Autowired
    private Resource fgdcNasaSciMeta;

    @Autowired
    private Resource fgdcNasaSysMeta;

    @Autowired
    private ScienceMetadataDocumentSubprocessor fgdcstd00111999Subprocessor;

    private IConverter dateConverter = new FgdcDateConverter();
    private IConverter solrDateConverter = new SolrDateConverter();

    // solr/rule field name (from spring context fields) mapped to actual value
    // from xml documents (system and science metadata docs)
    private HashMap<String, String> csiroExpected = new HashMap<String, String>();
    private HashMap<String, String> fgdcNasaExpected = new HashMap<String, String>();

    private String csiro_pid = "www.nbii.gov_metadata_mdata_CSIRO_csiro_d_abayadultprawns";
    private String nasa_pid = "www.nbii.gov_metadata_mdata_NASA_nasa_d_FEDGPS1293";

    @Before
    public void setUp() throws Exception {
        String hostname = InetAddress.getLocalHost().getCanonicalHostName();
        // science metadata
        csiroExpected
                .put("abstract",
                        "Adult prawn species, size, sex, reproductive stage, moult stage, and parasites were measured at 20 stations in Albatross Bay, Gulf of Carpentaria. Sampling was carried out monthly between 1986 and 1992. This metadata record is sourced from 'MarLIN', the CSIRO Marine Laboratories Information Network.");
        csiroExpected.put("beginDate", dateConverter.convert("19860301"));
        csiroExpected.put("class", "Malacostraca");
        csiroExpected.put("contactOrganization", "CSIRO Division of Marine Research-Hobart");
        csiroExpected.put("eastBoundCoord", "142.0");
        csiroExpected.put("westBoundCoord", "141.5");
        csiroExpected.put("southBoundCoord", "-13.0");
        csiroExpected.put("northBoundCoord", "-12.5");
        csiroExpected.put("edition", "");
        csiroExpected.put("endDate", dateConverter.convert("19920401"));
        csiroExpected.put("gcmdKeyword", "");
        csiroExpected.put("genus", "");
        csiroExpected.put("geoform", "maps data");
        csiroExpected.put("kingdom", "Animalia");
        csiroExpected.put("order", "Decapoda");
        csiroExpected.put("phylum", "Arthropoda");
        csiroExpected.put("species", "");
        csiroExpected.put("placeKey", "Australlia#Gulf of Carpentaria#Albatross Bay");
        csiroExpected.put("origin",
                "CSIRO Marine Research (formerly CSIRO Division of Fisheries/Fisheries Research)");
        csiroExpected.put("pubDate", dateConverter.convert("1993"));
        csiroExpected
                .put("purpose",
                        "The purpose of the dataset is to provide information about adult prawn species in Albatross Bay, Gulf of Carpentaria.");
        csiroExpected.put("title", "Albatross Bay Adult Prawn Data 1986-1992");
        csiroExpected.put("web_url", "");

        csiroExpected.put("keywords",
                "adult prawn data#size#sex#reproductive stage#moult stage#parasites");
        csiroExpected.put("fileID", "https://" + hostname + "/cn/v1/resolve/" + csiro_pid);

        // system metadata
        csiroExpected.put("id", csiro_pid);
        csiroExpected.put("objectformat", "FGDC-STD-001.1-1999");
        csiroExpected.put("size", "9008");
        csiroExpected.put("checksum", "86bc6417ef29b6fbd279160699044e5e");
        csiroExpected.put("checksumAlgorithm", "MD5");
        csiroExpected.put("submitter", "CN=Dave Vieglais T799,O=Google,C=US,DC=cilogon,DC=org");
        csiroExpected.put("rightsholder", "CN=Dave Vieglais T799,O=Google,C=US,DC=cilogon,DC=org");
        csiroExpected.put("rep_allowed", "true");
        csiroExpected.put("n_replicas", "3");
        csiroExpected.put("pref_rep_mn", "");
        csiroExpected.put("blocked_rep_mn", "");
        csiroExpected.put("obsoletes", "");
        csiroExpected.put("dateuploaded", solrDateConverter.convert("2012-03-22T13:55:48.348202"));
        csiroExpected.put("datemodified", solrDateConverter.convert("2012-03-22T13:55:48.360604"));
        csiroExpected.put("datasource", "test_documents");
        csiroExpected.put("auth_mn", "test_documents");
        csiroExpected.put("replica_mn", "");
        csiroExpected.put("replica_verified", "");
        csiroExpected.put("readPermission", "public");
        csiroExpected.put("writePermission",
                "CN=Dave Vieglais T799,O=Google,C=US,DC=cilogon,DC=org");
        csiroExpected.put("changePermission", "");
        csiroExpected.put("isPublic", "true");
        csiroExpected.put("data_url", "https://" + hostname + "/cn/v1/resolve/" + csiro_pid);

        /************************************************/
        /** Second test object expected data ************/
        // science metadata
        fgdcNasaExpected
                .put("abstract",
                        "The Biospheric Sciences Branch (formerly Earth Resources Branch) within the Laboratory for Terrestrial Physics at NASA's Goddard Space Flight Center and associated University investigators are involved in a research program entitled Forest Ecosystem Dynamics (FED) which is fundamentally concerned with vegetation change of forest ecosystems at local to regional spatial scales (100 to 10,000 meters) and temporal scales ranging from monthly to decadal periods (10 to 100 years). The nature and extent of the impacts of these changes, as well as the feedbacks to global climate, may be addressed through modeling the interactions of the vegetation, soil, and energy components of the boreal ecosystem. The Howland Forest research site lies within the Northern Experimental Forest of International Paper. The natural stands in this boreal-northern hardwood transitional forest consist of spruce-hemlock-fir, aspen-birch, and hemlock-hardwood mixtures. The topography of the region varies from flat to gently rolling, with a maximum elevation change of less than 68 m within 10 km. Due to the region's glacial history, soil drainage classes within a small area may vary widely, from well drained to poorly drained. Consequently, an elaborate patchwork of forest communities has developed, supporting exceptional local species diversity. This data set is in ARC/INFO export format and contains Global Positioning Systems (GPS) ground control points in and around the International Paper Experimental Forest, Howland ME.");
        fgdcNasaExpected.put("beginDate", dateConverter.convert("19931201"));
        fgdcNasaExpected.put("class", "");
        fgdcNasaExpected
                .put("contactOrganization",
                        "Forest Ecosystem Dynamics Project, Biospheric Sciences Branch, Hydrospheric and Biospheric Sciences Laboratory, Earth Sciences Division, Science and Exploration Directorate, Goddard Space Flight Center, NASA");
        fgdcNasaExpected.put("eastBoundCoord", "-68.0");
        fgdcNasaExpected.put("westBoundCoord", "-68.0");
        fgdcNasaExpected.put("southBoundCoord", "45.0");
        fgdcNasaExpected.put("northBoundCoord", "45.0");
        fgdcNasaExpected.put("edition", "");
        fgdcNasaExpected.put("endDate", dateConverter.convert("19931231"));
        fgdcNasaExpected
                .put("gcmdKeyword",
                        "EARTH SCIENCE > HUMAN DIMENSIONS > LAND USE/LAND COVER > LAND MANAGEMENT > GROUND CONTROL POINT#SATELLITES#GPS > GLOBAL POSITIONING SYSTEM#FED > FOREST ECOSYSTEM DYNAMICS");
        fgdcNasaExpected.put("genus", "");
        fgdcNasaExpected.put("geoform", "Maps and Data");
        fgdcNasaExpected.put("kingdom", "");
        fgdcNasaExpected.put("order", "");
        fgdcNasaExpected.put("phylum", "");
        fgdcNasaExpected.put("species", "");
        fgdcNasaExpected
                .put("placeKey",
                        "CONTINENT > NORTH AMERICA#CONTINENT > NORTH AMERICA > UNITED STATES OF AMERICA > MAINE");
        fgdcNasaExpected
                .put("origin",
                        "Elizabeth M. Nel; NASA Goddard Space Flight Center, Forest Ecosystem Dynamics Project");
        fgdcNasaExpected.put("pubDate", dateConverter.convert("1994"));
        fgdcNasaExpected
                .put("purpose",
                        "The field measurements component of the FED project was initiated to acquire data which are needed to: improve our understanding of vegetation, soil, and energy dynamics, and other biotic and abiotic processes within forested ecosystems so that the models can be parameterized, updated and modified; and to acquire in situ field observations for comparison with model results and conceptual model refinement.");
        fgdcNasaExpected
                .put("title",
                        "Global Positioning System Ground Control Points Acquired 1993 for the Forest Ecosystem Dynamics Project Spatial Data Archive");
        fgdcNasaExpected.put("web_url", "http://fedwww.gsfc.nasa.gov/");
        fgdcNasaExpected.put("keywords", "Easting#GIS#Northing");
        fgdcNasaExpected.put("fileID", "https://" + hostname + "/cn/v1/resolve/" + nasa_pid);

        // system metadata
        fgdcNasaExpected.put("id", nasa_pid);
        fgdcNasaExpected.put("objectformat", "FGDC-STD-001.1-1999");
        fgdcNasaExpected.put("size", "14880");
        fgdcNasaExpected.put("checksum", "c72ff66bbe7fa99e5fb399bab8cb6f85");
        fgdcNasaExpected.put("checksumAlgorithm", "MD5");
        fgdcNasaExpected.put("submitter", "CN=Dave Vieglais T799,O=Google,C=US,DC=cilogon,DC=org");
        fgdcNasaExpected.put("rightsholder",
                "CN=Dave Vieglais T799,O=Google,C=US,DC=cilogon,DC=org");
        fgdcNasaExpected.put("rep_allowed", "true");
        fgdcNasaExpected.put("n_replicas", "3");
        fgdcNasaExpected.put("pref_rep_mn", "");
        fgdcNasaExpected.put("blocked_rep_mn", "");
        fgdcNasaExpected.put("obsoletes", "");
        fgdcNasaExpected.put("dateuploaded",
                solrDateConverter.convert("2012-03-22T13:53:02.814057"));
        fgdcNasaExpected.put("datemodified",
                solrDateConverter.convert("2012-03-22T13:53:02.821757"));
        fgdcNasaExpected.put("datasource", "test_documents");
        fgdcNasaExpected.put("auth_mn", "test_documents");
        fgdcNasaExpected.put("replica_mn", "");
        fgdcNasaExpected.put("replica_verified", "");
        fgdcNasaExpected.put("readPermission", "public");
        fgdcNasaExpected.put("writePermission",
                "CN=Dave Vieglais T799,O=Google,C=US,DC=cilogon,DC=org");
        fgdcNasaExpected.put("changePermission", "");
        fgdcNasaExpected.put("isPublic", "true");
        fgdcNasaExpected.put("data_url", "https://" + hostname + "/cn/v1/resolve/" + nasa_pid);
    }

    /**
     * Testing that the Xpath expressions used by XPathParser and associates are
     * 'mining' the expected data from the science and system metadata
     * documents.
     * 
     * @throws Exception
     */
    @Test
    public void testCsiroScienceMetadataFields() throws Exception {

        Integer fieldCount = Integer.valueOf(0);

        Document scienceMetadataDoc = getXPathDocumentParser().generateSystemMetadataDoc(
                fdgc01111999SciMeta.getInputStream());
        for (SolrField field : fgdcstd00111999Subprocessor.getFieldList()) {
            boolean compared = compareFields(csiroExpected, scienceMetadataDoc, field, csiro_pid);
            if (compared) {
                fieldCount++;
            }
        }

        Document systemMetadataDoc = getXPathDocumentParser().generateSystemMetadataDoc(
                fdgc01111999SysMeta.getInputStream());
        for (SolrField field : getXPathDocumentParser().getFields()) {
            boolean compared = compareFields(csiroExpected, systemMetadataDoc, field, csiro_pid);
            if (compared) {
                fieldCount++;
            }
        }
        // if field count is off, some field did not get compared that should
        // have.
        Assert.assertEquals(csiroExpected.keySet().size(), fieldCount.intValue());
    }

    @Test
    public void testFgdcNasaScienceMetadataFields() throws Exception {

        Integer fieldCount = Integer.valueOf(0);

        Document scienceMetadataDoc = getXPathDocumentParser().generateSystemMetadataDoc(
                fgdcNasaSciMeta.getInputStream());
        for (SolrField field : fgdcstd00111999Subprocessor.getFieldList()) {
            boolean compared = compareFields(fgdcNasaExpected, scienceMetadataDoc, field, nasa_pid);
            if (compared) {
                fieldCount++;
            }
        }

        Document systemMetadataDoc = getXPathDocumentParser().generateSystemMetadataDoc(
                fgdcNasaSysMeta.getInputStream());
        for (SolrField field : getXPathDocumentParser().getFields()) {
            boolean compared = compareFields(fgdcNasaExpected, systemMetadataDoc, field, nasa_pid);
            if (compared) {
                fieldCount++;
            }
        }
        // if field count is off, some field did not get compared that should
        // have.
        Assert.assertEquals(fgdcNasaExpected.keySet().size(), fieldCount.intValue());
    }
}
