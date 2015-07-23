package org.dataone.cn.index;

import java.net.URLEncoder;
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
public class SolrFieldIsotc211Test extends BaseSolrFieldXPathTest {

    private static final String isotc211FormatId = "http://www.isotc211.org/2005/gmd";

    @Autowired
    private Resource isotc211_nodc_1_SysMeta;

    @Autowired
    private Resource isotc211_nodc_1_SciMeta;

    private String pid1 = "gov.noaa.nodc:9900233";

    @Autowired
    private Resource isotc211_nodc_2_SysMeta;

    @Autowired
    private Resource isotc211_nodc_2_SciMeta;

    private String pid2 = "gov.noaa.nodc:GHRSST-NEODAAS-L2P-AVHRR17_L";

    @Autowired
    private Resource isotc211_iarc_1_SysMeta;

    @Autowired
    private Resource isotc211_iarc_1_SciMeta;

    private String pid3 = "iso19139_8bd65007-f4b7-4b6e-8e71-05d7cf48a620_0";

    @Autowired
    private Resource isotc211_iarc_2_SysMeta;

    @Autowired
    private Resource isotc211_iarc_2_SciMeta;

    private String pid4 = "iso19139_bcc7e1be-2683-433c-b351-bc061f35ceb8_0";

    @Autowired
    private ScienceMetadataDocumentSubprocessor isotc211Subprocessor;

    private HashMap<String, String> nodc1Expected = new HashMap<String, String>();
    private HashMap<String, String> nodc2Expected = new HashMap<String, String>();

    private HashMap<String, String> iarc1Expected = new HashMap<String, String>();
    private HashMap<String, String> iarc2Expected = new HashMap<String, String>();

    private SolrDateConverter dateConverter = new SolrDateConverter();

    @Before
    public void setUp() throws Exception {
        setupNodc1Expected();
        setupNodc2Expected();
        setupIarc1Expected();
        setupIarc2Expected();
    }

    private void setupNodc1Expected() throws Exception {
        // science metadata
        nodc1Expected.put("author", "Alexander Sy");
        nodc1Expected.put("authorSurName", "Alexander Sy");
        nodc1Expected.put("authorSurNameSort", "Alexander Sy");
        nodc1Expected
                .put("origin",
                        "US National Oceanographic Data Center#Alexander Sy#DOC/NOAA/NESDIS/NODC > National Oceanographic Data Center, NESDIS, NOAA, U.S. Department of Commerce");
        nodc1Expected.put("investigator", "Alexander Sy");
        nodc1Expected.put("abstract", "");
        nodc1Expected
                .put("title",
                        "DEPTH - OBSERVATION and Other Data from UNKNOWN PLATFORMS and Other Platforms from 19980101 to 19981212 (NODC Accession 9900233)");
        nodc1Expected.put("pubDate", dateConverter.convert("2014-01-23T14:00:11"));
        nodc1Expected.put("beginDate", dateConverter.convert("1998-01-01"));
        nodc1Expected.put("endDate", dateConverter.convert("1998-12-12"));
        nodc1Expected
                .put("keywords",
                        "9900233#DEPTH - OBSERVATION#WATER TEMPERATURE#bathythermograph - XBT#physical#profile#ANTON DOHRN II#CAP FINISTERRE#GAUSS#KOELN EXPRESS#UNKNOWN PLATFORMS#University of Hamburg; Institut Fuer Meereskunde#University of Hamburg; Institut Fuer Meereskunde#UNKNOWN#WORLD OCEAN CIRCULATION EXPERIMENT (WOCE)#oceanography");

        nodc1Expected.put("contactOrganization", "US National Oceanographic Data Center");
        nodc1Expected.put("southBoundCoord", "-9");
        nodc1Expected.put("northBoundCoord", "65.7");
        nodc1Expected.put("westBoundCoord", "-50");
        nodc1Expected.put("eastBoundCoord", "-5.7");
        nodc1Expected.put("geohash_1", "e");
        nodc1Expected.put("geohash_2", "em");
        nodc1Expected.put("geohash_3", "emh");
        nodc1Expected.put("geohash_4", "emh1");
        nodc1Expected.put("geohash_5", "emh1q");
        nodc1Expected.put("geohash_6", "emh1q2");
        nodc1Expected.put("geohash_7", "emh1q2b");
        nodc1Expected.put("geohash_8", "emh1q2bn");
        nodc1Expected.put("geohash_9", "emh1q2bnx");
        nodc1Expected.put("fileID",
                "https://" + hostname + "/cn/v1/resolve/" + URLEncoder.encode(pid1, "UTF-8"));
        nodc1Expected
                .put("text",
                        "gov.noaa.nodc:9900233    eng    utf8    dataset      US National Oceanographic Data Center    Data Officer        301-713-3272    301-713-3302        1315 East-West Highway, SSMC3, 4th floor    Silver Spring    MD    20910-3282    USA    NODC.DataOfficer@noaa.gov        http://www.nodc.noaa.gov/    HTTP    Standard Internet browser    US National Oceanographic Data Center website    Main NODC website providing links to the NODC Geoportal and access links to data and data services.    information        custodian      2014-01-23T14:00:11    ISO 19115-2 Geographic Information - Metadata - Part 2: Extensions for Imagery and Gridded Data    ISO 19115-2:2009(E)        DEPTH - OBSERVATION and Other Data from UNKNOWN PLATFORMS and Other Platforms from 19980101 to 19981212 (NODC Accession 9900233)      2010-12-19    publication          NODC Accession Number       US National Oceanographic Data Center    resourceProvider        gov.noaa.nodc:9900233        US National Oceanographic Data Center        301-713-3277    301-713-3302        1315 East-West Highway, SSMC3, 4th floor    Silver Spring    MD    20910-3282    USA    NODC.DataOfficer@noaa.gov        http://www.nodc.noaa.gov/    HTTP    Standard Internet browser    US National Oceanographic Data Center website    Main NODC website providing links to the NODC Geoportal and access links to data and data services.    information        publisher        Alexander Sy    Federal Maritime Agency - Hamburg        040-3190-3430        BERNHARD-NOCHT-STRASSE 78    HAMBURG    D-20359    DEU    alexander.sy@bsh.de        http://www.bsh.de    HTTP    Standard Internet browser    Federal Maritime Agency - Hamburg website    Institution web page    information        resourceProvider        University of Hamburg; Institut Fuer Meereskunde        TROPLOWITZSTR, 7    HAMBURG    D-2000, 54    DEU        resourceProvider      tableDigital       BASIC RESEARCH    completed      US National Oceanographic Data Center    NODC User Services        301-713-3277    301-713-3302        1315 East-West Highway, SSMC3, 4th floor    Silver Spring    MD    20910-3282    USA    NODC.Services@noaa.gov        http://www.nodc.noaa.gov/    HTTP    Standard Internet browser    US National Oceanographic Data Center website    Main NODC website providing links to the NODC Geoportal and access links to data and data services.    information      8:30-6:00 PM, EST      pointOfContact        asNeeded        http://data.nodc.noaa.gov/cgi-bin/gfx?id=gov.noaa.nodc:9900233    Preview graphic    PNG        9900233      NODC ACCESSION NUMBER      2000-02-29    publication            DEPTH - OBSERVATION    WATER TEMPERATURE    theme      NODC DATA TYPES THESAURUS           bathythermograph - XBT    instrument      NODC INSTRUMENT TYPES THESAURUS           physical    profile    theme      NODC OBSERVATION TYPES THESAURUS           ANTON DOHRN II    CAP FINISTERRE    GAUSS    KOELN EXPRESS    UNKNOWN PLATFORMS    platform      NODC PLATFORM NAMES THESAURUS           University of Hamburg; Institut Fuer Meereskunde    dataCenter      NODC COLLECTING INSTITUTION NAMES THESAURUS           University of Hamburg; Institut Fuer Meereskunde    dataCenter      NODC SUBMITTING INSTITUTION NAMES THESAURUS           UNKNOWN    WORLD OCEAN CIRCULATION EXPERIMENT (WOCE)    project      NODC PROJECT NAMES THESAURUS           oceanography    theme      WMO_CategoryCode      2012-09-15    publication            Please note: NOAA and NODC make no warranty, expressed or implied, regarding these data, nor does the fact of distribution constitute such a warranty. NOAA and NODC cannot assume liability for any damages caused by any errors or omissions in these data.    accessLevel: Public        otherRestrictions    Cite as: Sy, A. and University of Hamburg; Institut Fuer Meereskunde (2010). DEPTH - OBSERVATION and Other Data from UNKNOWN PLATFORMS and Other Platforms from 19980101 to 19981212 (NODC Accession 9900233). National Oceanographic Data Center, NOAA. Dataset. [access date]        otherRestrictions    None      eng    utf8    oceans    environment        -50    -5.7    -9    65.7         1998-01-01  1998-12-12         Note: Metadata for this accession were extracted from a legacy databasemaintained by the U.S. National Oceanographic Data Center (NODC). Thedesign of the database did not exactly reflect the FGDC ContentStandard for Digital Geospatial Metadata (CSDGM).Principal Investigator (PI) and organization contact informationaccurately represents all available information from the legacy databaseat the time that this description was created. However, properattribution of a PI to a specific institution or the role (submitting orcollecting) taken by an institution may not be correct due to inexactmapping between fields in the legacy database and the CSDGM. Due to thisuncertainty, the contact information was initially recorded in theSupplemental Information element of the CSDGM description.To develop more accurate metadata, the NODC reviews metadata for allaccessions on an ongoing basis.Points of contact for this data set include:Contact info:Agency: UNIVERSITY OF HAMBURG; INSTITUT FUER MEERESKUNDEPI: Sy, Dr. AlexanderAddress:address: TROPLOWITZSTR, 7city: HAMBURGstate: NOT AVAILABLEpostal: D-2000, 54country: GERMANY            US National Oceanographic Data Center    NODC User Services        301-713-3277    301-713-3302        1315 East-West Highway, SSMC3, 4th floor    Silver Spring    MD    20910-3282    USA    NODC.Services@noaa.gov      8:30-6:00 PM, EST      pointOfContact        Digital data may be downloaded from NODC at no charge in most cases. For custom orders of digital data or to obtain a copy of analog materials, please contact NODC User Services for information about current fees.    Data may be searched and downloaded using online services provided by the NODC using the online resource URLs in this record. Contact NODC User Services for custom orders. When requesting data from the NODC, the desired data set may be referred to by the NODC Accession Number listed in this metadata record.        Originator data format           http://accession.nodc.noaa.gov/9900233    HTTP    Standard Internet browser    Details    Navigate directly to the URL for a descriptive web page with download links.    information            http://accession.nodc.noaa.gov/oas/9900233    HTTP    Standard Internet browser    Metadata    Navigate directly to the URL for a descriptive web page with download links.    information          79.252      http://accession.nodc.noaa.gov/download/9900233    HTTP    Standard Internet browser    Download    Navigate directly to the URL for data access and direct download.    download          79.252      ftp://ftp.nodc.noaa.gov/nodc/archive/arc0001/9900233/    FTP    Any FTP client    FTP    These data are available through the File Transfer Protocol (FTP). The base URL of NODC's FTP server is ftp://ftp.nodc.noaa.gov/ and you may use any FTP client to download these data.    download              asNeeded    Metadata are developed, maintained and distributed by the NODC. Updates are performed as needed to maintain currentness.      DOC/NOAA/NESDIS/NODC > National Oceanographic Data Center, NESDIS, NOAA, U.S. Department of Commerce    custodian gov.noaa.nodc:9900233");

        // system metadata
        nodc1Expected.put("id", pid1);
        nodc1Expected.put("seriesId", "");
        nodc1Expected.put("formatId", isotc211FormatId);
        nodc1Expected.put("formatType", "");
        // nodc1Expected.put("formatType", "METADATA");
        nodc1Expected.put("size", "11406");
        nodc1Expected.put("checksum", "ff5d7c92a8c3285f49a8f216f929f14c6b5335a3");
        nodc1Expected.put("checksumAlgorithm", "SHA-1");
        nodc1Expected.put("submitter", "NODC");
        nodc1Expected.put("rightsHolder", "NODC");
        nodc1Expected.put("replicationAllowed", "true");
        nodc1Expected.put("numberReplicas", "3");
        nodc1Expected.put("preferredReplicationMN", "");
        nodc1Expected.put("blockedReplicationMN", "");
        nodc1Expected.put("obsoletes", "");
        nodc1Expected.put("obsoletedBy", "");
        nodc1Expected.put("dateUploaded", dateConverter.convert("2015-05-08T01:47:41.356045"));
        nodc1Expected.put("dateModified", dateConverter.convert("2015-05-08T01:47:41.391065Z"));
        nodc1Expected.put("datasource", "urn:node:NODC");
        nodc1Expected.put("authoritativeMN", "urn:node:NODC");
        nodc1Expected.put("replicaMN", "");
        nodc1Expected.put("replicaVerifiedDate", "");
        nodc1Expected.put("readPermission", "public");
        nodc1Expected.put("writePermission", "");
        nodc1Expected.put("changePermission", "");
        nodc1Expected.put("isPublic", "true");
        nodc1Expected.put("dataUrl",
                "https://" + hostname + "/cn/v1/resolve/" + URLEncoder.encode(pid1, "UTF-8"));
    }

    private void setupNodc2Expected() throws Exception {
        // science metadata
        nodc2Expected.put("author", "Peter Miller");
        nodc2Expected.put("authorSurName", "Peter Miller");
        nodc2Expected.put("authorSurNameSort", "Peter Miller");
        nodc2Expected
                .put("origin",
                        "US National Oceanographic Data Center#Peter Miller#NEODAAS > NERC Earth Observation Data Acquisition and Analysis Service#Edward M. Armstrong#NEODAAS#Group for High Resolution Sea Surface Temperature#DOC/NOAA/NESDIS/NODC > National Oceanographic Data Center, NESDIS, NOAA, U.S. Department of Commerce#NASA/JPL/PODAAC > Physical Oceanography Distributed Active Archive Center, Jet Propulsion Laboratory, NASA");
        nodc2Expected.put("investigator", "Peter Miller#Edward M. Armstrong#NEODAAS");
        nodc2Expected
                .put("abstract",
                        "A Level 2P swath-based Group for High Resolution Sea Surface Temperature (GHRSST) dataset for the North Atlantic area from the Advanced Very High Resolution Radiometer (AVHRR) on the NOAA-17 platform (launched on 24 June 2002). This particular dataset is produced by the Natural Environment Research Council (NERC) Earth Observation Data Acquisition and Analysis Service (NEODAAS) in collaboration with the National Centre for Ocean Forecasting (NCOF) in the United Kingdom. The AVHRR is a space-borne scanning sensor on the National Oceanic and Atmospheric Administration (NOAA) family of Polar Orbiting Environmental Satellites (POES) having a operational legacy that traces back to the Television Infrared Observation Satellite-N (TIROS-N) launched in 1978. AVHRR instruments measure the radiance of the Earth in 5 (or 6) relatively wide spectral bands. The first two are centered around the red (0.6 micrometer) and near-infrared (0.9 micrometer) regions, the third one is located around 3.5 micrometer, and the last two sample the emitted thermal radiation, around 11 and 12 micrometers, respectively. The legacy 5 band instrument is known as AVHRR/2 while the more recent version, the AVHRR/3 (first carried on the NOAA-15 platform), acquires data in a 6th channel located at 1.6 micrometer. Typically the 11 and 12 micron channels are used to derive sea surface temperature (SST) sometimes in combination with the 3.5 micron channel. The highest ground resolution that can be obtained from the current AVHRR instruments is 1.1 km at nadir. The NOAA platforms are sun synchronous generally viewing the same earth location twice a day or more (latitude dependent) due to the relatively large AVHRR swath of approximately 2400 km."
                                + "NEODAAS-Dundee acquires approximately 15 AVHRR direct broadcast High Resolution Picture Transmission (HRPT) passes per day over NW Europe and the Arctic. Each pass is approximately 15 minutes duration. These are immediately transferred to NEODAAS-Plymouth where they are processed into sea surface temperature (SST) products and converted to L2P specifications.");
        nodc2Expected
                .put("title",
                        "GHRSST Level 2P North Atlantic Regional Bulk Sea Surface Temperature from the Advanced Very High Resolution Radiometer (AVHRR) on the NOAA-17 satellite produced by NEODAAS (GDS version 1)");
        nodc2Expected.put("pubDate", dateConverter.convert("2015-02-04T21:33:10"));
        nodc2Expected.put("beginDate", dateConverter.convert("2008-09-02"));
        nodc2Expected.put("endDate", dateConverter.convert("2010-05-18"));
        nodc2Expected
                .put("keywords",

                        "0046179#0046210#0046244#0046274#0046313#0046344#0046375#0046406#0046443#0046470#0046502#0046532#0046565#0046592#0046629#0046659#0046690#0046720#0046764#0046794#0046824#0046856#0046886#0046913#0046942#0046972#0047003#0047033#0047066#0047096#0047126#0047155#0047182#0048372#0048399#0048429#0048460#0048488#0048504#0048550#0048579#0048608#0048637#0048668#0048699#0048740#0048767#0048793#0048819#0048847#0048873#0048904#0048932#0048961#0048988#0049016#0049044#0049072#0049098#0049126#0049154#0049178#0049202#0049230#0049258#0049286#0049295#0049338#0049362#0049388#0049413#0049445#0049474#0049531#0049576#0049605#0049633#0049663#0049769#0049796#0049824#0049851#0049918#0049945#0049999#0050026#0050053#0050080#0050107#0050134#0050160#0050183#0050219#0050249#0050277#0050300#0050332#0050359#0050387#0050413#0050439#0050461#0050549#0050572#0050597#0050623#0050648#0050718#0050765#0050829#0050853#0050878#0050903#0050928#0050950#0050990#0051014#0051039#0051094#0051119#0051144#0051169#0051194#0051216#0051241#0051266#0051291#0051316#0051341#0051366#0051391#0051418#0051443#0051470#0051492#0051521#0051546#0051575#0051600#0051627#0051652#0051677#0051701#0051726#0051748#0051773#0051798#0051996#0052051#0052084#0052112#0052139#0052164#0052189#0052215#0052238#0052262#0052286#0052312#0052335#0052360#0052386#0052421#0052446#0052494#0052518#0052541#0052566#0052595#0052616#0052638#0052668#0052693#0052718#0052744#0052787#0052810#0052833#0052859#0052889#0052916#0052943#0052966#0052999#0053022#0053084#0053108#0053128#0053152#0053176#0053199#0053232#0053251#0053295#0053326#0053348#0053372#0053396#0053430#0053454#0053478#0053512#0053539#0053565#0053591#0053618#0053645#0053678#0053705#0053733#0053760#0053790#0053817#0053842#0053869#0053895#0053922#0053948#0053972#0053996#0054019#0054045#0054071#0054100#0054127#0054161#0054188#0054338#0054367#0054398#0054427#0054455#0054482#0054518#0054547#0054579#0054608#0054637#0054666#0054694#0054749#0054793#0054822#0054914#0054942#0054993#0055021#0055052#0055081#0055117#0055146#0055178#0055204#0055231#0055258#0055315#0055343#0055428#0055458#0055488#0055518#0055548#0055577#0055607#0055637#0055668#0055695#0055725#0055755#0055852#0055883#0055912#0055940#0055968#0055996#0056024#0056053#0056085#0056115#0056146#0056176#0056225#0056255#0056287#0056317#0056348#0056378#0056412#0056442#0056470#0056500#0056527#0056560#0056590#0056618#0056645#0056674#0056703#0056738#0056767#0056798#0056827#0056862#0056891#0056923#0056952#0056978#0057004#0057032#0057059#0057088#0057117#0057149#0057178#0057208#0057237#0057266#0057295#0057326#0057355#0057388#0057417#0057444#0057480#0057509#0057538#0057567#0057599#0057628#0057670#0057699#0057727#0057755#0057881#0057909#0057939#0057964#0057989#0058017#0058047#0058075#0058109#0058135#0058163#0058191#0058220#0058248#0058280#0058308#0058336#0058363#0058389#0058416#0058447#0058475#0058505#0058533#0058561#0058589#0058617#0058642#0058669#0058693#0058720#0058744#0058768#0058792#0058818#0058865#0058889#0058914#0058938#0058965#0058992#0059022#0059049#0059078#0059105#0059136#0059162#0059187#0059209#0059232#0059257#0059283#0059308#0059332#0059367#0059398#0059425#0059450#0059475#0059501#0059528#0059554#0059584#0059609#0059635#0059658#0059681#0059704#0059717#0059721#0059727#0059791#0059792#0059800#0059804#0059828#0059833#0060201#0060228#0060252#0060275#0060321#0060344#0060367#0060416#0060439#0060462#0060488#0060514#0060540#0060566#0060592#0060616#0060641#0060666#0060695#0060720#0060767#0060791#0060817#0060841#0060866#0060891#0060916#0060940#0060970#0060996#0061021#0061051#0061076#0061102#0061128#0061159#0061185#0061214#0061240#0061268#0061294#0061320#0061346#0061373#0061399#0061428#0061454#0061481#0061507#0061533#0061559#0061592#0061618#0061644#0061669#0061695#0061721#0061758#0061784#0061811#0061839#0061865#0061889#0061913#0061938#0061962#0061990#0062014#0062045#0062069#0062094#0062118#0062145#0062170#0062196#0062220#0062244#0062267#0062293#0062317#0062343#0062367#0062391#0062415#0062442#0062476#0062493#0062521#0062547#0063068#0063079#0063090#0063101#0063111#0063121#0063131#0063141#0063151#0063162#0063173#0063197#0063209#0063221#0063233#0063249#0063277#0063289#0063304#0063316#0063328#0063340#0063352#0063364#0063376#0063390#0063402#0063414#0063436#0063449#0063462#0063474#0063487#0063500#0063513#0063541#0063554#0063567#0063589#0063602#0063619#0063792#0063839#0063863#0063908#0063979#0064188#0064470#0064678#0065176#0066694#0073287#0073288#0073291#0073294#0087331#0114635#0114637#0122474#SEA SURFACE TEMPERATURE#AVHRR-3#satellite data#NOAA-17 SATELLITE#NERC Earth Observation Data Acquisition and Analysis Service#US NASA; Jet Propulsion Laboratory; Physical Oceanography Distributed Active Archive Center#Group for High Resolution Sea Surface Temperature (GHRSST)#World-Wide Distribution#oceanography#Earth Science > Oceans > Ocean Temperature > Sea Surface Temperature > Bulk Sea Surface Temperature#Northeast Atlantic");
        nodc2Expected.put("contactOrganization", "US National Oceanographic Data Center");
        nodc2Expected.put("southBoundCoord", "24");
        nodc2Expected.put("northBoundCoord", "90");
        nodc2Expected.put("westBoundCoord", "-60");
        nodc2Expected.put("eastBoundCoord", "60");
        nodc2Expected.put("geohash_1", "u");
        nodc2Expected.put("geohash_2", "u4");
        nodc2Expected.put("geohash_3", "u40");
        nodc2Expected.put("geohash_4", "u40h");
        nodc2Expected.put("geohash_5", "u40h2");
        nodc2Expected.put("geohash_6", "u40h20");
        nodc2Expected.put("geohash_7", "u40h208");
        nodc2Expected.put("geohash_8", "u40h2081");
        nodc2Expected.put("geohash_9", "u40h20810");
        nodc2Expected.put("fileID",
                "https://" + hostname + "/cn/v1/resolve/" + URLEncoder.encode(pid2, "UTF-8"));
        nodc2Expected
                .put("text",
                        "gov.noaa.nodc:GHRSST-NEODAAS-L2P-AVHRR17_L    eng    utf8    series      US National Oceanographic Data Center    Data Officer        301-713-3272    301-713-3302        1315 East-West Highway, SSMC3, 4th floor    Silver Spring    MD    20910-3282    USA    NODC.DataOfficer@noaa.gov        http://www.nodc.noaa.gov/    HTTP    Standard Internet browser    US National Oceanographic Data Center website    Main NODC website providing links to the NODC Geoportal and access links to data and data services.    information        custodian        Peter Miller    NEODAAS > NERC Earth Observation Data Acquisition and Analysis Service    Technical Contact        +44 1752 633485    +44 1752 633101        info@nospam.neodaas.ac.uk      Phone/FAX/E-mail      pointOfContact      2015-02-04T21:33:10    ISO 19115-2 Geographic Information - Metadata - Part 2: Extensions for Imagery and Gridded Data    ISO 19115-2:2009(E)      2      column     1.1        row     1.1      area    true          GHRSST Level 2P North Atlantic Regional Bulk Sea Surface Temperature from the Advanced Very High Resolution Radiometer (AVHRR) on the NOAA-17 satellite produced by NEODAAS (GDS version 1)    Sea surface temperature from AVHRR      2014-10-02    publication      1        NODC Collection Identifier       US National Oceanographic Data Center    resourceProvider        gov.noaa.nodc:GHRSST-NEODAAS-L2P-AVHRR17_L        US National Oceanographic Data Center        301-713-3277    301-713-3302        1315 East-West Highway, SSMC3, 4th floor    Silver Spring    MD    20910-3282    USA    NODC.DataOfficer@noaa.gov        http://www.nodc.noaa.gov/    HTTP    Standard Internet browser    US National Oceanographic Data Center website    Main NODC website providing links to the NODC Geoportal and access links to data and data services.    information        publisher        Edward M. Armstrong    US NASA; Jet Propulsion Laboratory        (818) 393-6710        MS 300/320 4800 Oak Grove Drive    Pasadena    CA    91109    USA    edward.m.armstrong@jpl.nasa.gov        resourceProvider        US NASA; Jet Propulsion Laboratory; Physical Oceanography Distributed Active Archive Center        626-744-5508        4800 Oak Grove Drive    Pasadena    CA    91109    USA    podaac@podaac.jpl.nasa.gov        http://podaac.jpl.nasa.gov/index.html    HTTP    Standard Internet browser    NASA Jet Propulsion Laboratory PO DAAC website    Institution web page    information        resourceProvider        NERC Earth Observation Data Acquisition and Analysis Service        +44 1752 633485        Plymouth Marine Laboratory Prospect Place    Plymouth    Devon    PL1 3DH    GBR    info@neodaas.ac.uk        http://www.neodaas.ac.uk/    HTTP    Standard Internet browser    NEODAAS website    Institution web page    information        originator      tableDigital      A Level 2P swath-based Group for High Resolution Sea Surface Temperature (GHRSST) dataset for the North Atlantic area from the Advanced Very High Resolution Radiometer (AVHRR) on the NOAA-17 platform (launched on 24 June 2002). This particular dataset is produced by the Natural Environment Research Council (NERC) Earth Observation Data Acquisition and Analysis Service (NEODAAS) in collaboration with the National Centre for Ocean Forecasting (NCOF) in the United Kingdom. The AVHRR is a space-borne scanning sensor on the National Oceanic and Atmospheric Administration (NOAA) family of Polar Orbiting Environmental Satellites (POES) having a operational legacy that traces back to the Television Infrared Observation Satellite-N (TIROS-N) launched in 1978. AVHRR instruments measure the radiance of the Earth in 5 (or 6) relatively wide spectral bands. The first two are centered around the red (0.6 micrometer) and near-infrared (0.9 micrometer) regions, the third one is located around 3.5 micrometer, and the last two sample the emitted thermal radiation, around 11 and 12 micrometers, respectively. The legacy 5 band instrument is known as AVHRR/2 while the more recent version, the AVHRR/3 (first carried on the NOAA-15 platform), acquires data in a 6th channel located at 1.6 micrometer. Typically the 11 and 12 micron channels are used to derive sea surface temperature (SST) sometimes in combination with the 3.5 micron channel. The highest ground resolution that can be obtained from the current AVHRR instruments is 1.1 km at nadir. The NOAA platforms are sun synchronous generally viewing the same earth location twice a day or more (latitude dependent) due to the relatively large AVHRR swath of approximately 2400 km.NEODAAS-Dundee acquires approximately 15 AVHRR direct broadcast High Resolution Picture Transmission (HRPT) passes per day over NW Europe and the Arctic. Each pass is approximately 15 minutes duration. These are immediately transferred to NEODAAS-Plymouth where they are processed into sea surface temperature (SST) products and converted to L2P specifications.    BASIC RESEARCH    Processed by Panorama at PML    onGoing      US National Oceanographic Data Center    NODC User Services        301-713-3277    301-713-3302        1315 East-West Highway, SSMC3, 4th floor    Silver Spring    MD    20910-3282    USA    NODC.Services@noaa.gov        http://www.nodc.noaa.gov/    HTTP    Standard Internet browser    US National Oceanographic Data Center website    Main NODC website providing links to the NODC Geoportal and access links to data and data services.    information      8:30-6:00 PM, EST      pointOfContact        Peter Miller    NEODAAS > NERC Earth Observation Data Acquisition and Analysis Service        +44 1752 633485    +44 1752 633101        info@nospam.neodaas.ac.uk        http://www.neodaas.ac.uk/faq/        pointOfContact        asNeeded        http://data.nodc.noaa.gov/cgi-bin/gfx?id=gov.noaa.nodc:GHRSST-NEODAAS-L2P-AVHRR17_L    Preview graphic    PNG        0046179    0046210    0046244    0046274    0046313    0046344    0046375    0046406    0046443    0046470    0046502    0046532    0046565    0046592    0046629    0046659    0046690    0046720    0046764    0046794    0046824    0046856    0046886    0046913    0046942    0046972    0047003    0047033    0047066    0047096    0047126    0047155    0047182    0048372    0048399    0048429    0048460    0048488    0048504    0048550    0048579    0048608    0048637    0048668    0048699    0048740    0048767    0048793    0048819    0048847    0048873    0048904    0048932    0048961    0048988    0049016    0049044    0049072    0049098    0049126    0049154    0049178    0049202    0049230    0049258    0049286    0049295    0049338    0049362    0049388    0049413    0049445    0049474    0049531    0049576    0049605    0049633    0049663    0049769    0049796    0049824    0049851    0049918    0049945    0049999    0050026    0050053    0050080    0050107    0050134    0050160    0050183    0050219    0050249    0050277    0050300    0050332    0050359    0050387    0050413    0050439    0050461    0050549    0050572    0050597    0050623    0050648    0050718    0050765    0050829    0050853    0050878    0050903    0050928    0050950    0050990    0051014    0051039    0051094    0051119    0051144    0051169    0051194    0051216    0051241    0051266    0051291    0051316    0051341    0051366    0051391    0051418    0051443    0051470    0051492    0051521    0051546    0051575    0051600    0051627    0051652    0051677    0051701    0051726    0051748    0051773    0051798    0051996    0052051    0052084    0052112    0052139    0052164    0052189    0052215    0052238    0052262    0052286    0052312    0052335    0052360    0052386    0052421    0052446    0052494    0052518    0052541    0052566    0052595    0052616    0052638    0052668    0052693    0052718    0052744    0052787    0052810    0052833    0052859    0052889    0052916    0052943    0052966    0052999    0053022    0053084    0053108    0053128    0053152    0053176    0053199    0053232    0053251    0053295    0053326    0053348    0053372    0053396    0053430    0053454    0053478    0053512    0053539    0053565    0053591    0053618    0053645    0053678    0053705    0053733    0053760    0053790    0053817    0053842    0053869    0053895    0053922    0053948    0053972    0053996    0054019    0054045    0054071    0054100    0054127    0054161    0054188    0054338    0054367    0054398    0054427    0054455    0054482    0054518    0054547    0054579    0054608    0054637    0054666    0054694    0054749    0054793    0054822    0054914    0054942    0054993    0055021    0055052    0055081    0055117    0055146    0055178    0055204    0055231    0055258    0055315    0055343    0055428    0055458    0055488    0055518    0055548    0055577    0055607    0055637    0055668    0055695    0055725    0055755    0055852    0055883    0055912    0055940    0055968    0055996    0056024    0056053    0056085    0056115    0056146    0056176    0056225    0056255    0056287    0056317    0056348    0056378    0056412    0056442    0056470    0056500    0056527    0056560    0056590    0056618    0056645    0056674    0056703    0056738    0056767    0056798    0056827    0056862    0056891    0056923    0056952    0056978    0057004    0057032    0057059    0057088    0057117    0057149    0057178    0057208    0057237    0057266    0057295    0057326    0057355    0057388    0057417    0057444    0057480    0057509    0057538    0057567    0057599    0057628    0057670    0057699    0057727    0057755    0057881    0057909    0057939    0057964    0057989    0058017    0058047    0058075    0058109    0058135    0058163    0058191    0058220    0058248    0058280    0058308    0058336    0058363    0058389    0058416    0058447    0058475    0058505    0058533    0058561    0058589    0058617    0058642    0058669    0058693    0058720    0058744    0058768    0058792    0058818    0058865    0058889    0058914    0058938    0058965    0058992    0059022    0059049    0059078    0059105    0059136    0059162    0059187    0059209    0059232    0059257    0059283    0059308    0059332    0059367    0059398    0059425    0059450    0059475    0059501    0059528    0059554    0059584    0059609    0059635    0059658    0059681    0059704    0059717    0059721    0059727    0059791    0059792    0059800    0059804    0059828    0059833    0060201    0060228    0060252    0060275    0060321    0060344    0060367    0060416    0060439    0060462    0060488    0060514    0060540    0060566    0060592    0060616    0060641    0060666    0060695    0060720    0060767    0060791    0060817    0060841    0060866    0060891    0060916    0060940    0060970    0060996    0061021    0061051    0061076    0061102    0061128    0061159    0061185    0061214    0061240    0061268    0061294    0061320    0061346    0061373    0061399    0061428    0061454    0061481    0061507    0061533    0061559    0061592    0061618    0061644    0061669    0061695    0061721    0061758    0061784    0061811    0061839    0061865    0061889    0061913    0061938    0061962    0061990    0062014    0062045    0062069    0062094    0062118    0062145    0062170    0062196    0062220    0062244    0062267    0062293    0062317    0062343    0062367    0062391    0062415    0062442    0062476    0062493    0062521    0062547    0063068    0063079    0063090    0063101    0063111    0063121    0063131    0063141    0063151    0063162    0063173    0063197    0063209    0063221    0063233    0063249    0063277    0063289    0063304    0063316    0063328    0063340    0063352    0063364    0063376    0063390    0063402    0063414    0063436    0063449    0063462    0063474    0063487    0063500    0063513    0063541    0063554    0063567    0063589    0063602    0063619    0063792    0063839    0063863    0063908    0063979    0064188    0064470    0064678    0065176    0066694    0073287    0073288    0073291    0073294    0087331    0114635    0114637    0122474      NODC ACCESSION NUMBER      2014-10-02    publication            SEA SURFACE TEMPERATURE    theme      NODC DATA TYPES THESAURUS           AVHRR-3    instrument      NODC INSTRUMENT TYPES THESAURUS           satellite data    theme      NODC OBSERVATION TYPES THESAURUS           NOAA-17 SATELLITE    platform      NODC PLATFORM NAMES THESAURUS           NERC Earth Observation Data Acquisition and Analysis Service    dataCenter      NODC COLLECTING INSTITUTION NAMES THESAURUS           US NASA; Jet Propulsion Laboratory; Physical Oceanography Distributed Active Archive Center    dataCenter      NODC SUBMITTING INSTITUTION NAMES THESAURUS           Group for High Resolution Sea Surface Temperature (GHRSST)    project      NODC PROJECT NAMES THESAURUS           World-Wide Distribution    place      NODC SEA AREA NAMES THESAURUS           oceanography    theme      WMO_CategoryCode      2012-09-15    publication            Earth Science > Oceans > Ocean Temperature > Sea Surface Temperature > Bulk Sea Surface Temperature    theme      NASA/GCMD Earth Science Keywords     Olsen, L.M., G. Major, K. Shein, J. Scialdone, S. Ritz, T. Stevens, M. Morahan, A. Aleman, R. Vogel, S. Leicester, H. Weir, M. Meaux, S. Grebas, C. Solomon, M. Holland, T. Northcutt, R. A. Restrepo, R. Bilodeau (2013). NASA/Global Change Master Directory (GCMD) Earth Science Keywords. Version 8.0.0.0.0          Northeast Atlantic    place      NASA/GCMD Location Keywords     Olsen, L.M., G. Major, K. Shein, J. Scialdone, S. Ritz, T. Stevens, M. Morahan, A. Aleman, R. Vogel, S. Leicester, H. Weir, M. Meaux, S. Grebas, C. Solomon, M. Holland, T. Northcutt, R. A. Restrepo, R. Bilodeau (2013). NASA/Global Change Master Directory (GCMD) Earth Science Keywords. Version 8.0.0.0.0          Please note: NOAA and NODC make no warranty, expressed or implied, regarding these data, nor does the fact of distribution constitute such a warranty. NOAA and NODC cannot assume liability for any damages caused by any errors or omissions in these data.    accessLevel: Public        otherRestrictions    Cite as: NERC Earth Observation Data Acquisition and Analysis Service (NEODAAS) (2008). GHRSST Level 2P North Atlantic Regional Bulk Sea Surface Temperature from the Advanced Very High Resolution Radiometer (AVHRR) on the NOAA-17 satellite produced by NEODAAS (GDS version 1). National Oceanographic Data Center, NOAA. Dataset.  [access date]        otherRestrictions    None          GHRSST Level 2P North Atlantic Regional Bulk Sea Surface Temperature from the Advanced Very High Resolution Radiometer (AVHRR) on the NOAA-17 satellite produced by NEODAAS    Sea surface temperature from AVHRR      20080526    creation      1      NEODAAS        http://www.neodaas.ac.uk/faq/        originator        NEODAAS        NERC Earth Observation Data Acquisition and Analysis Service, UK        publisher        collection    userGuide          Portal to the GHRSST Global Data Assembly Center and data access       Group for High Resolution Sea Surface Temperature        http://ghrsst.jpl.nasa.gov    HTTP    Standard Internet browser    Portal to the GHRSST Global Data Assembly Center and data access    Portal to the GHRSST Global Data Assembly Center and data access        custodian        crossReference    collection          GHRSST Project homepage       Group for High Resolution Sea Surface Temperature        http://www.ghrsst.org    HTTP    Standard Internet browser    GHRSST Project homepage    GHRSST Project homepage        custodian        crossReference    collection          Web Service (PO.DAAC Labs) (Search Granule)       Group for High Resolution Sea Surface Temperature        http://podaac.jpl.nasa.gov/ws/search/granule/?datasetId=PODAAC-GH17L-2PS01&apidoc    HTTP    Standard Internet browser    Web Service (PO.DAAC Labs)    (Search Granule)        custodian        crossReference    collection      grid    eng    utf8    oceans    environment    biota    climatologyMeteorologyAtmosphere        -60    60    24    90         2008-09-02  2010-05-18                 referenceInformation        lat      float              lon      float              time      int                  US National Oceanographic Data Center    NODC User Services        301-713-3277    301-713-3302        1315 East-West Highway, SSMC3, 4th floor    Silver Spring    MD    20910-3282    USA    NODC.Services@noaa.gov      8:30-6:00 PM, EST      pointOfContact        Digital data may be downloaded from NODC at no charge in most cases. For custom orders of digital data or to obtain a copy of analog materials, please contact NODC User Services for information about current fees.    Data may be searched and downloaded using online services provided by the NODC using the online resource URLs in this record. Contact NODC User Services for custom orders. When requesting data from the NODC, the desired data set may be referred to by the NODC Accession Number listed in this metadata record.        netCDF    netCDF-3    BZIP2          http://www.nodc.noaa.gov/geoportal/rest/find/document?searchText=fileIdentifier%3AGHRSST-NEODAAS-L2P-AVHRR17_L*%20OR%20fileIdentifier%3ANEODAAS-L2P-AVHRR17_L*&start=1&max=100&f=SearchPage    HTTP    Standard Internet browser    Granule Search    Granule Search    search            http://data.nodc.noaa.gov/cgi-bin/iso?id=gov.noaa.nodc:GHRSST-NEODAAS-L2P-AVHRR17_L    HTTP    Standard Internet browser    Details    Navigate directly to the URL for a descriptive web page with download links.    information            http://data.nodc.noaa.gov/thredds/catalog/ghrsst/L2P/AVHRR17_L/NEODAAS/    THREDDS    Standard Internet browsers can browse THREDDS Data Servers and specialized THREDDS software can enable more sophisticated data access and visualizations.    THREDDS    These data are available through a variety of services via a THREDDS (Thematic Real-time Environmental Distributed Data Services) Data Server (TDS). The base URL of NODC's TDS is http://data.nodc.noaa.gov/thredds. Depending on the dataset, the TDS can provide WMS, WCS, DAP, HTTP, and other data access and metadata services as well. For more information on the TDS, see http://www.unidata.ucar.edu/software/thredds/current/tds/.    download            http://data.nodc.noaa.gov/opendap/ghrsst/L2P/AVHRR17_L/NEODAAS/    DAP    Standard Internet browsers can browse OPeNDAP servers and specialized OPeNDAP software can enable more sophisticated data access and visualizations.    OPeNDAP    These data are available through the Data Access Protocol (DAP) via an OPeNDAP Hyrax server. The base URL of NODC's Hyrax server is http://data.nodc.noaa.gov/opendap/. For a listing of OPeNDAP clients which may be used to access OPeNDAP-enabled data sets, please see the OPeNDAP website at http://opendap.org/.    download            http://data.nodc.noaa.gov/ghrsst/L2P/AVHRR17_L/NEODAAS/    HTTP    Standard Internet browser    Download    Navigate directly to the URL for data access and direct download.    download            ftp://ftp.nodc.noaa.gov/pub/data.nodc/ghrsst/L2P/AVHRR17_L/NEODAAS/    FTP    Any FTP client    FTP    These data are available through the File Transfer Protocol (FTP). The base URL of NODC's FTP server is ftp://ftp.nodc.noaa.gov/ and you may use any FTP client to download these data.    download              asNeeded    Combined metadata from JPL and NODC      DOC/NOAA/NESDIS/NODC > National Oceanographic Data Center, NESDIS, NOAA, U.S. Department of Commerce    custodian              AVHRR-3 > Advanced Very High Resolution Radiometer-3      sensor    The AVHRR is a radiation-detection imager that can be used for remotely determining cloud cover and the surface temperature. Note that the term surface can mean the surface of the Earth, the upper surfaces of clouds, or the surface of a body of water.          NOAA-17 > National Oceanic & Atmospheric Administration-17      The POES satellite system offers the advantage of daily global coverage, with morning and afternoon orbits that deliver global data, for improvement of weather forecasting. The information received includes cloud cover, storm location, temperature, and heat balance in the earth's atmosphere.      NEODAAS        http://www.neodaas.ac.uk/faq/        sponsor        NASA/JPL/PODAAC > Physical Oceanography Distributed Active Archive Center, Jet Propulsion Laboratory, NASA        http://podaac.jpl.nasa.gov    information        sponsor gov.noaa.nodc:GHRSST-NEODAAS-L2P-AVHRR17_L");

        // system metadata
        nodc2Expected.put("id", pid2);
        nodc2Expected.put("seriesId", "");
        nodc2Expected.put("formatId", isotc211FormatId);
        nodc2Expected.put("formatType", "");
        //odc2Expected.put("formatType", "METADATA");
        nodc2Expected.put("size", "22406");
        nodc2Expected.put("checksum", "ee5d7c92a8c3285f49a8f216f929f14c6b51d5a3");
        nodc2Expected.put("checksumAlgorithm", "SHA-1");
        nodc2Expected.put("submitter", "NODC");
        nodc2Expected.put("rightsHolder", "NODC");
        nodc2Expected.put("replicationAllowed", "true");
        nodc2Expected.put("numberReplicas", "2");
        nodc2Expected.put("preferredReplicationMN", "");
        nodc2Expected.put("blockedReplicationMN", "");
        nodc2Expected.put("obsoletes", "");
        nodc2Expected.put("obsoletedBy", "");
        nodc2Expected.put("dateUploaded", dateConverter.convert("2015-05-08T01:47:41.356045"));
        nodc2Expected.put("dateModified", dateConverter.convert("2015-05-08T01:47:41.391065Z"));
        nodc2Expected.put("datasource", "urn:node:NODC");
        nodc2Expected.put("authoritativeMN", "urn:node:NODC");
        nodc2Expected.put("replicaMN", "");
        nodc2Expected.put("replicaVerifiedDate", "");
        nodc2Expected.put("readPermission", "public");
        nodc2Expected.put("writePermission", "");
        nodc2Expected.put("changePermission", "");
        nodc2Expected.put("isPublic", "true");
        nodc2Expected.put("dataUrl",
                "https://" + hostname + "/cn/v1/resolve/" + URLEncoder.encode(pid2, "UTF-8"));
    }

    private void setupIarc1Expected() throws Exception {
        // science metadata
        iarc1Expected.put("author", "Jake Stroh");
        iarc1Expected.put("authorSurName", "Jake Stroh");
        iarc1Expected.put("authorSurNameSort", "Jake Stroh");
        iarc1Expected.put("origin", "Jake Stroh#International Arctic Research Center");
        iarc1Expected.put("investigator", "Jake Stroh");
        iarc1Expected
                .put("abstract",
                        "The rich collection of BEST-BSIERP observations and other sources of data provide an excellent opportunity for synthesis through modeling and data assimilation to improve our understanding of changes in physical forcings of the Bering ecosystem in response to climate change. Assimilating data of different origins, which may be sparse in space and time, is difficult using simple algorithms (traditional optimal interpolation, correlation analysis etc.)."
                                + "The 4Dvar approach is effective for performing spatiotemporal interpolation of sparse data via interpolation (covariance) functions with scales based on ocean dynamics (Bennett, 2002).");
        iarc1Expected
                .put("title",
                        "Volume, heat and salt transport in the North-Eastern Bering Sea during 2007-2010 derived through the 4dvar data assimilation of in-situ and satellite observations");
        iarc1Expected.put("pubDate", dateConverter.convert("2014-03-04T10:48:09"));
        iarc1Expected.put("beginDate", dateConverter.convert("2007-01-01T10:43:00"));
        iarc1Expected.put("endDate", dateConverter.convert("2010-12-31T10:44:00"));
        iarc1Expected.put("keywords", "BEST#Bering sea#BSIERP");
        iarc1Expected.put("contactOrganization", "International Arctic Research Center");
        iarc1Expected.put("southBoundCoord", "50");
        iarc1Expected.put("northBoundCoord", "67");
        iarc1Expected.put("westBoundCoord", "-180");
        iarc1Expected.put("eastBoundCoord", "-158");
        iarc1Expected.put("geohash_1", "b");
        iarc1Expected.put("geohash_2", "b4");
        iarc1Expected.put("geohash_3", "b4r");
        iarc1Expected.put("geohash_4", "b4ru");
        iarc1Expected.put("geohash_5", "b4ruf");
        iarc1Expected.put("geohash_6", "b4ruf3");
        iarc1Expected.put("geohash_7", "b4ruf39");
        iarc1Expected.put("geohash_8", "b4ruf39g");
        iarc1Expected.put("geohash_9", "b4ruf39gn");
        iarc1Expected.put("fileID",
                "https://" + hostname + "/cn/v1/resolve/" + URLEncoder.encode(pid3, "UTF-8"));
        iarc1Expected
                .put("text",
                        "8bd65007-f4b7-4b6e-8e71-05d7cf48a620    eng         Jake Stroh                                       jnstroh@alaska.edu             2014-03-04T10:48:09    ISO 19115:2003/19139    1.0        Volume, heat and salt transport in the North-Eastern Bering Sea during 2007-2010 derived through the 4dvar data assimilation of in-situ and satellite observations      2014-02-21T10:40:00              The rich collection of BEST-BSIERP observations and other sources of data provide an excellent opportunity for synthesis through modeling and data assimilation to improve our understanding of changes in physical forcings of the Bering ecosystem in response to climate change. Assimilating data of different origins, which may be sparse in space and time, is difficult using simple algorithms (traditional optimal interpolation, correlation analysis etc.).The 4Dvar approach is effective for performing spatiotemporal interpolation of sparse data via interpolation (covariance) functions with scales based on ocean dynamics (Bennett, 2002).      Jake Stroh    International Arctic Research Center                                    jnstroh@alaska.edu                         thumbnail           large_thumbnail        BEST           Bering sea           BSIERP               eng       geoscientificInformation         2007-01-01T10:43:00  2010-12-31T10:44:00             -180    -158    50    67                http://climate.iarc.uaf.edu:8080/geonetwork/srv/en/resources.get?id=611&fname=best_bs_currents3-rev_final.pdf&access=private    WWW:DOWNLOAD-1.0-http--download    best_bs_currents3-rev_final.pdf    Ocean Sciences meeting 2014 iso19139_8bd65007-f4b7-4b6e-8e71-05d7cf48a620_0");

        // system metadata
        iarc1Expected.put("id", pid3);
        iarc1Expected.put("seriesId", "");
        iarc1Expected.put("formatId", isotc211FormatId);
        iarc1Expected.put("formatType", "");
        //iarc1Expected.put("formatType", "METADATA");
        iarc1Expected.put("size", "12917");
        iarc1Expected.put("checksum", "037282cbe6fdfc99ed9f3b49a0ddb05d38e11704");
        iarc1Expected.put("checksumAlgorithm", "SHA-1");
        iarc1Expected
                .put("submitter", "CN=jlong,O=International Arctic Research Center,ST=AK,C=US");
        iarc1Expected.put("rightsHolder",
                "CN=jlong,O=International Arctic Research Center,ST=AK,C=US");
        iarc1Expected.put("replicationAllowed", "true");
        iarc1Expected.put("numberReplicas", "3");
        iarc1Expected.put("preferredReplicationMN", "");
        iarc1Expected.put("blockedReplicationMN", "");
        iarc1Expected.put("obsoletes", "");
        iarc1Expected.put("obsoletedBy", "");
        iarc1Expected.put("dateUploaded", dateConverter.convert("2015-05-08T01:47:46.858771"));
        iarc1Expected.put("dateModified", dateConverter.convert("2015-05-08T01:47:46.893356Z"));
        iarc1Expected.put("datasource", "urn:node:IARC");
        iarc1Expected.put("authoritativeMN", "urn:node:IARC");
        iarc1Expected.put("replicaMN", "");
        iarc1Expected.put("replicaVerifiedDate", "");
        iarc1Expected.put("readPermission", "public");
        iarc1Expected.put("writePermission", "");
        iarc1Expected.put("changePermission", "");
        iarc1Expected.put("isPublic", "true");
        iarc1Expected.put("dataUrl",
                "https://" + hostname + "/cn/v1/resolve/" + URLEncoder.encode(pid3, "UTF-8"));
    }

    private void setupIarc2Expected() throws Exception {
        // science metadata
        iarc2Expected.put("author", "Dr Jessica Cherry");
        iarc2Expected.put("authorSurName", "Dr Jessica Cherry");
        iarc2Expected.put("authorSurNameSort", "Dr Jessica Cherry");
        iarc2Expected.put("origin",
                "Dr Jessica Cherry#Water and Environmental Research Center, UAF");
        iarc2Expected.put("investigator", "Dr Jessica Cherry");
        iarc2Expected
                .put("abstract",
                        "This aerial photograph collection consists of about 1897 frames of color infrared and natural color transparencies.  It was flown in the summers of 1976 and 1977 for the Outer Continental Shelf Environmental Assessment Program, at an altitude ranging from about 5,000 - 18,400 feet.  ECR prefix indicates color infrared, and CC/ZC indicates natural color.  It is filed by flight line.This subset of the aerial photograph collection includes flight lines that included coastline from the Seward Peninsula.  Flight lines include numbers 8-14 and 24-40.  There are 846 images in this subset, 281 natural color images and 565 color infrared images.The photography may be ordered from the National Archives or duplicated in the National Ocean Service photo lab.");
        iarc2Expected
                .put("title",
                        "National Ocean Service Aerial Photographs: Seward Peninsula Coastline - Flight Line 29");
        iarc2Expected.put("pubDate", dateConverter.convert("2011-12-08T15:38:44"));
        iarc2Expected.put("beginDate", dateConverter.convert("1976-07-23T00:00:00"));
        iarc2Expected.put("endDate", dateConverter.convert("1976-07-23T00:00:00"));
        iarc2Expected
                .put("keywords",
                        "Seward Peninsula#Alaska#Coast#Norton Bay#Candle#Aerial#Photography#Color#Infrared");
        iarc2Expected.put("contactOrganization", "Water and Environmental Research Center, UAF");
        iarc2Expected.put("southBoundCoord", "64.34");
        iarc2Expected.put("northBoundCoord", "65.07");
        iarc2Expected.put("westBoundCoord", "-161.09");
        iarc2Expected.put("eastBoundCoord", "-160.55");
        iarc2Expected.put("geohash_1", "b");
        iarc2Expected.put("geohash_2", "b7");
        iarc2Expected.put("geohash_3", "b7t");
        iarc2Expected.put("geohash_4", "b7t8");
        iarc2Expected.put("geohash_5", "b7t8h");
        iarc2Expected.put("geohash_6", "b7t8h7");
        iarc2Expected.put("geohash_7", "b7t8h7n");
        iarc2Expected.put("geohash_8", "b7t8h7nm");
        iarc2Expected.put("geohash_9", "b7t8h7nmy");
        iarc2Expected.put("fileID",
                "https://" + hostname + "/cn/v1/resolve/" + URLEncoder.encode(pid4, "UTF-8"));
        iarc2Expected
                .put("text",
                        "bcc7e1be-2683-433c-b351-bc061f35ceb8    eng       6a503c5a-78ae-434e-b76e-b2d2755b7aef      Dr Jessica Cherry              907-474-5730                          jcherry@iarc.uaf.edu             2011-12-08T15:38:44    ISO 19115:2003/19139    1.0        WGS 1984            National Ocean Service Aerial Photographs: Seward Peninsula Coastline - Flight Line 29    Outer Continental Shelf Environmental Assesment Program      1976-07-23T00:00:00                 This aerial photograph collection consists of about 1897 frames of color infrared and natural color transparencies.  It was flown in the summers of 1976 and 1977 for the Outer Continental Shelf Environmental Assessment Program, at an altitude ranging from about 5,000 - 18,400 feet.  ECR prefix indicates color infrared, and CC/ZC indicates natural color.  It is filed by flight line.This subset of the aerial photograph collection includes flight lines that included coastline from the Seward Peninsula.  Flight lines include numbers 8-14 and 24-40.  There are 846 images in this subset, 281 natural color images and 565 color infrared images.The photography may be ordered from the National Archives or duplicated in the National Ocean Service photo lab.    Photograph Alaskan coast line during 1976-1977 for comparison at later dates.            Dr Jessica Cherry    Water and Environmental Research Center, UAF           907-474-5730              Fairbanks    Alaska       United States of America    jcherry@iarc.uaf.edu                      Seward Peninsula    Alaska    Coast    Norton Bay    Candle           Aerial    Photography    Color    Infrared                         eng       imageryBaseMapsEarthCover         1976-07-23T00:00:00  1976-07-23T00:00:00             -161.09    -160.55    64.34    65.07        Seward Peninsula CoastlineFlight Line 29Color: Ungalik River - Kenwood CreekInfrared: Ungalik River - KoyukQuad: Norton Bay, Candle  1st Frame - Last FrameColor:     6539 - 6546Infrared: 5760 - 5770Altitude: 18000 feet          film    none          http://climate.iarc.uaf.edu:8080/geonetwork/srv/en/resources.get?id=169&fname=&access=private    WWW:DOWNLOAD-1.0-http--download       This is not where this data is stored.  Please see Point of Contact for information. iso19139_bcc7e1be-2683-433c-b351-bc061f35ceb8_0");

        // system metadata
        iarc2Expected.put("id", pid4);
        iarc2Expected.put("seriesId", "");
        iarc2Expected.put("formatId", isotc211FormatId);
        iarc2Expected.put("formatType", "");
        // iarc2Expected.put("formatType", "METADATA");
        iarc2Expected.put("size", "15406");
        iarc2Expected.put("checksum", "225d7c92a8c3285f49a8f216f929f14c6b51d5a3");
        iarc2Expected.put("checksumAlgorithm", "SHA-1");
        iarc2Expected
                .put("submitter", "CN=jlong,O=International Arctic Research Center,ST=AK,C=US");
        iarc2Expected.put("rightsHolder",
                "CN=jlong,O=International Arctic Research Center,ST=AK,C=US");
        iarc2Expected.put("replicationAllowed", "true");
        iarc2Expected.put("numberReplicas", "3");
        iarc2Expected.put("preferredReplicationMN", "");
        iarc2Expected.put("blockedReplicationMN", "");
        iarc2Expected.put("obsoletes", "");
        iarc2Expected.put("obsoletedBy", "");
        iarc2Expected.put("dateUploaded", dateConverter.convert("2015-05-08T01:47:41.356045"));
        iarc2Expected.put("dateModified", dateConverter.convert("2015-05-08T01:47:41.391065Z"));
        iarc2Expected.put("datasource", "urn:node:IARC");
        iarc2Expected.put("authoritativeMN", "urn:node:IARC");
        iarc2Expected.put("replicaMN", "");
        iarc2Expected.put("replicaVerifiedDate", "");
        iarc2Expected.put("readPermission", "public");
        iarc2Expected.put("writePermission", "");
        iarc2Expected.put("changePermission", "");
        iarc2Expected.put("isPublic", "true");
        iarc2Expected.put("dataUrl",
                "https://" + hostname + "/cn/v1/resolve/" + URLEncoder.encode(pid4, "UTF-8"));

    }

    @Test
    public void testIsotc211Nodc1FieldParsing() throws Exception {
        testXPathParsing(isotc211Subprocessor, isotc211_nodc_1_SysMeta, isotc211_nodc_1_SciMeta,
                nodc1Expected, pid1);
    }

    @Test
    public void testIsotc211Nodc2FieldParsing() throws Exception {
        testXPathParsing(isotc211Subprocessor, isotc211_nodc_2_SysMeta, isotc211_nodc_2_SciMeta,
                nodc2Expected, pid2);
    }

    @Test
    public void testIsotc211Iarc1FieldParsing() throws Exception {
        testXPathParsing(isotc211Subprocessor, isotc211_iarc_1_SysMeta, isotc211_iarc_1_SciMeta,
                iarc1Expected, pid3);
    }

    @Test
    public void testIsotc211Iarc2FieldParsing() throws Exception {
        testXPathParsing(isotc211Subprocessor, isotc211_iarc_2_SysMeta, isotc211_iarc_2_SciMeta,
                iarc2Expected, pid4);
    }
}
