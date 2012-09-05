package org.dataone.cn.index;

import java.net.InetAddress;
import java.util.HashMap;

import org.dataone.cn.indexer.convert.IConverter;
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
public class SolrFieldXPathDryadTest extends BaseSolrFieldXPathTest {

    @Autowired
    private Resource dryadSciMeta1;

    @Autowired
    private Resource dryadSysMeta1;

    @Autowired
    private Resource dryadSciMeta2;

    @Autowired
    private Resource dryadSysMeta2;

    @Autowired
    private ScienceMetadataDocumentSubprocessor dryadSubprocessor;

    private IConverter dateConverter = new SolrDateConverter();

    // solr/rule field name (from spring context fields) mapped to actual value
    // from xml documents (system and science metadata docs)
    private HashMap<String, String> dryad1Expected = new HashMap<String, String>();

    private String dryad1Pid = "http:/dx.doi.org/10.5061/dryad.9025";
    private String dryad1PidEncoded = "http%3A%2Fdx.doi.org%2F10.5061%2Fdryad.9025";

    private HashMap<String, String> dryad2Expected = new HashMap<String, String>();

    private String dryad2Pid = "http:/dx.doi.org/10.5061/dryad.9025/1";
    private String dryad2PidEncoded = "http%3A%2Fdx.doi.org%2F10.5061%2Fdryad.9025%2F1";

    @Before
    public void setUp() throws Exception {
        String hostname = InetAddress.getLocalHost().getCanonicalHostName();
        // science metadata
        dryad1Expected
                .put("abstract",
                        "The genus Stomoxys Geoffroy (Diptera; Muscidae) contains species of parasitic flies that are of medical and economic importance. We conducted a phylogenetic analysis including 10 representative species of the genus including multiple exemplars, together with the closely related genera Prostomoxys Zumpt, Haematobosca Bezzi, and Haematobia Lepeletier & Serville. Phylogenetic relationships were inferred using maximum likelihood and Bayesian methods from DNA fragments from the cytochrome c oxidase subunit I (COI, 753 bp) and cytochrome b (CytB, 587 bp) mitochondrial genes, and the nuclear ribosomal internal transcribed spacer 2 (ITS2, 426 bp). The combination of mitochondrial and nuclear data strongly supports the paraphyly of the genus Stomoxys because of the inclusion of Prostomoxys saegerae Zumpt. This unexpected result suggests that Prostomoxys should be renamed into Stomoxys. Also, the deep molecular divergence observed between the subspecies Stomoxys niger niger Macquart and S. niger bilineatus Grünbreg led us to propose that they should rather be considered as distinct species, in agreement with ecological data. Bayesian phylogenetic analyses support three distinct lineages within the genus Stomoxys with a strong biogeographical component. The first lineage consists solely of the divergent Asian species S. indicus Picard which appears as the sister-group to all remaining Stomoxys species. The second clade groups the strictly African species Stomoxys inornatus Grünbreg, Stomoxys transvittatus Villeneuve, Stomoxys omega Newstead, and Stomoxys pallidus Roubaud. Finally, the third clade includes both African occurring and more widespread species such as the livestock pest Stomoxys calcitrans Linnaeus. Divergence time estimates indicate that the genus Stomoxys originated in the late Oligocene around 30 million years ago, with the major lineages diversifying in the Early Miocene between 20 and 15 million years ago at a time when temperate forests developed in the Northern Hemisphere.");
        dryad1Expected.put("site", "Worldwide#Everywhere");
        dryad1Expected
                .put("origin",
                        "Dsouli, Najla#Delsuc, Frédéric#Michaux, Johan#De Stordeur, Eric#Couloux, Arnaud#Veuille, Michel#Duvallet, Gérard");
        dryad1Expected.put("author", "Dsouli, Najla");
        dryad1Expected
                .put("investigator",
                        "Dsouli, Najla#Delsuc, Frédéric#Michaux, Johan#De Stordeur, Eric#Couloux, Arnaud#Veuille, Michel#Duvallet, Gérard");
        dryad1Expected.put("pubDate", dateConverter.convert("2011-03-31T15:39:44Z"));

        dryad1Expected.put("scientificName",
                "Stomoxyini#Stomoxys#Prostomoxys#Diptera#Muscidae#Haematobia#Haematobosca");
        dryad1Expected
                .put("title",
                        "Phylogenetic analyses of mitochondrial and nuclear data in haematophagous flies support the paraphyly of the genus Stomoxys (Diptera: Muscidae)");
        dryad1Expected.put("keywords",
                "Stomoxys flies#Phylogenetic relationship#Molecular dating#Dryad submission");
        dryad1Expected.put("fileID", "https://" + hostname + "/cn/v1/resolve/" + dryad1PidEncoded);
        dryad1Expected
                .put("text",
                        "publication  deposited  published  Dsouli, Najla  Delsuc, Frédéric  Michaux, Johan  De Stordeur, Eric  Couloux, Arnaud  Veuille, Michel  Duvallet, Gérard  2011-02-13  Phylogenetic analyses of mitochondrial and nuclear data in haematophagous flies support the paraphyly of the genus Stomoxys (Diptera: Muscidae)  Infection, Genetics and Evolution    doi:10.1016/j.meegid.2011.02.004  doi:10.5061/dryad.9025    package                  2011-03-31T15:39:44Z    Data from: Phylogenetic analyses of mitochondrial and nuclear data in haematophagous flies support the paraphyly of the genus Stomoxys (Diptera: Muscidae)    The genus Stomoxys Geoffroy (Diptera; Muscidae) contains species of parasitic flies that are of medical and economic importance. We conducted a phylogenetic analysis including 10 representative species of the genus including multiple exemplars, together with the closely related genera Prostomoxys Zumpt, Haematobosca Bezzi, and Haematobia Lepeletier & Serville. Phylogenetic relationships were inferred using maximum likelihood and Bayesian methods from DNA fragments from the cytochrome c oxidase subunit I (COI, 753 bp) and cytochrome b (CytB, 587 bp) mitochondrial genes, and the nuclear ribosomal internal transcribed spacer 2 (ITS2, 426 bp). The combination of mitochondrial and nuclear data strongly supports the paraphyly of the genus Stomoxys because of the inclusion of Prostomoxys saegerae Zumpt. This unexpected result suggests that Prostomoxys should be renamed into Stomoxys. Also, the deep molecular divergence observed between the subspecies Stomoxys niger niger Macquart and S. niger bilineatus Grünbreg led us to propose that they should rather be considered as distinct species, in agreement with ecological data. Bayesian phylogenetic analyses support three distinct lineages within the genus Stomoxys with a strong biogeographical component. The first lineage consists solely of the divergent Asian species S. indicus Picard which appears as the sister-group to all remaining Stomoxys species. The second clade groups the strictly African species Stomoxys inornatus Grünbreg, Stomoxys transvittatus Villeneuve, Stomoxys omega Newstead, and Stomoxys pallidus Roubaud. Finally, the third clade includes both African occurring and more widespread species such as the livestock pest Stomoxys calcitrans Linnaeus. Divergence time estimates indicate that the genus Stomoxys originated in the late Oligocene around 30 million years ago, with the major lineages diversifying in the Early Miocene between 20 and 15 million years ago at a time when temperate forests developed in the Northern Hemisphere.  Stomoxys flies  Phylogenetic relationship  Molecular dating  Dryad submission  Stomoxyini  Stomoxys  Prostomoxys  Diptera  Muscidae  Haematobia  Haematobosca  Worldwide  Everywhere  Cenozoic    doi:10.5061/dryad.9025/1  doi:10.5061/dryad.9025/2 http:/dx.doi.org/10.5061/dryad.9025");
        // system metadata
        dryad1Expected.put("id", dryad1Pid);
        dryad1Expected.put("formatId", "http://purl.org/dryad/terms/");
        dryad1Expected.put("size", "");
        dryad1Expected.put("checksum", "22899f46ed9f2176958b9da4d39f4117");
        dryad1Expected.put("checksumAlgorithm", "MD5");
        dryad1Expected.put("submitter", "187");
        dryad1Expected.put("rightsHolder", "frederic.delsuc@univ-montp2.fr");
        dryad1Expected.put("replicationAllowed", "");
        dryad1Expected.put("numberReplicas", "");
        dryad1Expected.put("preferredReplicationMN", "");
        dryad1Expected.put("blockedReplicationMN", "");
        dryad1Expected.put("obsoletes", "");
        dryad1Expected.put("dateUploaded", "");
        dryad1Expected.put("dateModified", "");
        dryad1Expected.put("datasource", "http://datadryad.org/mn/");
        dryad1Expected.put("authoritativeMN", "http://datadryad.org/mn/");
        dryad1Expected.put("replicaMN", "");
        dryad1Expected.put("replicaVerifiedDate", "");
        dryad1Expected.put("readPermission", "public");
        dryad1Expected.put("writePermission", "");
        dryad1Expected.put("changePermission", "");
        dryad1Expected.put("isPublic", "true");
        dryad1Expected.put("dataUrl", "https://" + hostname + "/cn/v1/resolve/" + dryad1PidEncoded);

        //
        //
        //
        dryad2Expected
                .put("abstract",
                        "Nexus file for the concatenation of 2 mitochondrial (COX1 and CYTB) and 1 nuclear (ITS2) genes for 39 dipterans.");
        dryad2Expected.put("site", "Worldwide");
        dryad2Expected
                .put("origin",
                        "Dsouli, Najla#Delsuc, Frédéric#Michaux, Johan#De Stordeur, Eric#Couloux, Arnaud#Veuille, Michel#Duvallet, Gérard");
        dryad2Expected.put("author", "Dsouli, Najla");
        dryad2Expected
                .put("investigator",
                        "Dsouli, Najla#Delsuc, Frédéric#Michaux, Johan#De Stordeur, Eric#Couloux, Arnaud#Veuille, Michel#Duvallet, Gérard");
        dryad2Expected.put("pubDate", dateConverter.convert("2011-03-31T15:44:45Z"));
        dryad2Expected.put("title", "Dsouli-InfectGenetEvol11 nexus file");
        dryad2Expected.put("keywords",
                "Stomoxys flies#Phylogenetic relationship#Molecular dating#Dryad submission");
        dryad2Expected.put("fileID", "https://" + hostname + "/cn/v1/resolve/" + dryad2PidEncoded);
        dryad2Expected.put("scientificName",
                "Stomoxyini#Stomoxys#Prostomoxys#Diptera#Muscidae#Haematobia#Haematobosca");

        dryad2Expected
                .put("text",
                        "file  deposited  Dsouli, Najla  Delsuc, Frédéric  Michaux, Johan  De Stordeur, Eric  Couloux, Arnaud  Veuille, Michel  Duvallet, Gérard  Dsouli-InfectGenetEvol11 nexus file  http://dx.doi.org/10.5061/dryad.9025/1  http://creativecommons.org/publicdomain/zero/1.0/  Nexus file for the concatenation of 2 mitochondrial (COX1 and CYTB) and 1 nuclear (ITS2) genes for 39 dipterans.  Stomoxys flies  Phylogenetic relationship  Molecular dating  Dryad submission  Stomoxyini  Stomoxys  Prostomoxys  Diptera  Muscidae  Haematobia  Haematobosca  Worldwide  Cenozoic  2011-03-31T15:44:45Z    Made available in DSpace on 2011-03-31T15:44:45Z (GMT). No. of bitstreams: 1Dsouli-InfectGenetEvol11.nex: 68337 bytes, checksum: 712866783892594e09b2e2294966f529 (MD5)  doi:10.5061/dryad.9025 http:/dx.doi.org/10.5061/dryad.9025/1");
        // system metadata
        dryad2Expected.put("id", dryad2Pid);
        dryad2Expected.put("formatId", "http://purl.org/dryad/terms/");
        dryad2Expected.put("size", "");
        dryad2Expected.put("checksum", "22899f46ed9f2176958b9da4d39f4117");
        dryad2Expected.put("checksumAlgorithm", "MD5");
        dryad2Expected.put("submitter", "187");
        dryad2Expected.put("rightsHolder", "frederic.delsuc@univ-montp2.fr");
        dryad2Expected.put("replicationAllowed", "");
        dryad2Expected.put("numberReplicas", "");
        dryad2Expected.put("preferredReplicationMN", "");
        dryad2Expected.put("blockedReplicationMN", "");
        dryad2Expected.put("obsoletes", "");
        dryad2Expected.put("dateUploaded", "");
        dryad2Expected.put("dateModified", "");
        dryad2Expected.put("datasource", "http://datadryad.org/mn/");
        dryad2Expected.put("authoritativeMN", "http://datadryad.org/mn/");
        dryad2Expected.put("replicaMN", "");
        dryad2Expected.put("replicaVerifiedDate", "");
        dryad2Expected.put("readPermission", "public");
        dryad2Expected.put("writePermission", "");
        dryad2Expected.put("changePermission", "");
        dryad2Expected.put("isPublic", "true");
        dryad2Expected.put("dataUrl", "https://" + hostname + "/cn/v1/resolve/" + dryad2PidEncoded);
    }

    /**
     * Testing that the Xpath expressions used by XPathParser and associates are
     * 'mining' the expected data from the science and system metadata
     * documents.
     * 
     * @throws Exception
     */
    @Test
    public void testDryadScienceMetadataFields() throws Exception {
        testXPathParsing(dryadSubprocessor, dryadSysMeta1, dryadSciMeta1, dryad1Expected, dryad1Pid);

        testXPathParsing(dryadSubprocessor, dryadSysMeta2, dryadSciMeta2, dryad2Expected, dryad2Pid);
    }

}
