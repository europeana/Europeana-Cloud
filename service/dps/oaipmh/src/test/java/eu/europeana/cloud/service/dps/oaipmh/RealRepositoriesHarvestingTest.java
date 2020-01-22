package eu.europeana.cloud.service.dps.oaipmh;

import eu.europeana.cloud.service.dps.Harvest;
import eu.europeana.cloud.service.dps.OAIHeader;
import org.dspace.xoai.model.oaipmh.Header;
import org.dspace.xoai.model.oaipmh.Verb;
import org.dspace.xoai.serviceprovider.exceptions.OAIRequestException;
import org.dspace.xoai.serviceprovider.parameters.GetRecordParameters;
import org.dspace.xoai.serviceprovider.parameters.ListIdentifiersParameters;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;

@Ignore
@RunWith(Parameterized.class)
public class RealRepositoriesHarvestingTest {

    private static final int DEFAULT_RETRIES = 3;
    private static final int SLEEP_TIME = 5*1000;

    private final String endpoint;
    private final String schema;
    private final String set;

    @Parameters
    public static Collection<Object[]> data() {
        Object[][] data = new Object[][]{
               /* {"http://islandskort.is/oai", "edm", ""},
                {"http://baekur.is/oai", "edm", ""},
                {"http://adl2.kb.dk/oai", "ese", ""}, //cannot harvest records - GetRecord method doesn't work, identifiers like "/hertz11_lev"
                {"http://panic.image.ece.ntua.gr:9000/photography/oai", "rdf", ""}, //extremely slow - might not work
                {"http://oai-02.kb.dk/oai/provider", "ese", ""},
                {"http://oai.kb.dk/oai/provider", "ese", ""},
                {"http://oai.kb.dk/oai/provider", "ese", ""},
                {"http://147.91.246.189/doiserbia_oaidc/oai2.aspx", "oai_dc", "tel"},
                {"http://1914-1918.europeana.eu/oai", "oai_dc", "story:ugc"},
                {"http://aggregator.vbanos.gr/", "ese", "490"},
                {"http://colleccions.eicub.net/oai/1", "edm", ""},
                {"http://dasi.humnet.unipi.it/de/cgi-bin/dasi-oai.pl", "edm", "obj_img_set"},
                {"http://data.jewisheritage.net/oai/OAIHandler", "edm", "bibliothèque_medem-maison_de_la_culture_yiddish"},
                {"http://data.museums.eu/oai/OAIHandler", "edm", "galerija_bozidar_jakac_-_kostanjevica_na_krki"},
                {"http://digitalna.nsk.hr/oai", "ese", "TEL"},
                {"http://digitool.bibnat.ro/OAI-PUB/", "oai_dc", ""},
                {"http://diglit.ub.uni-heidelberg.de/cgi-bin/digioai.cgi", "mets", "druckschriften"},
                {"http://dlib.mk/oai/request", "oai_dc", "com_68275_66"},
                {"http://europeana.cbuc.cat/oai.php", "ese", "manuscritAB"},
                {"http://formula1.csc.fi/oai-pmh", "edm", "musketti"},
                {"http://haw.nsk.hr/oai/", "oai_dc", ""},
                {"http://kobson.nb.rs/oaiphd/oai2.aspx", "oai_dc", "phd"},
                {"http://more.locloud.eu:8080/carare", "EDM", "SNHB"},
                {"http://more.locloud.eu:8080/locloud", "EDM", "IPCHS"},
                {"http://museion.esbirky.cz/w4m/oai/MRK", "ese", "SMUH"},
                {"http://nukoai.nuk.uni-lj.si:8089/repox/OAIHandler", "edm", "Rise_of_Literacy_in_Europe_part_1"},
                {"http://oai-bdb.onb.ac.at/Script/oai2.aspx", "ese", "9846792"},
                {"http://oaipmh.lnb.lv/repox/OAIHandler", "ese", "zlpics"},
                {"http://repox.kamra.si:8080/repox/OAIHandler", "edm", "Europeana_Kamra_Multimedia"},
                {"http://stary.webumenia.sk/oai-pmh", "ese", "Europeana SNG"},
                {"http://test117.ait.co.at/oai-provider-edm/oai/", "edm", "MNHN"},*/
                {"http://test117.ait.co.at/oai-provider-edm/oai/", "edm", "ZFMK"} /*,
                {"http://timarit.is/oai", "edm", "publications"},
                {"http://www.kulturpool.at/rest/export/1.0/oai-pmh", "edm", "oeaw"},
                {"http://www.manuscriptorium.com/oai/", "ese", "digitized"},
                {"https://aggregator.ekt.gr/aggregator-oai/request", "edm", "Digital_Pylia"},
                {"https://cdk.lib.cas.cz/oai", "ese", ""},
                {"https://dr.nsk.hr/oai", "oai_dc", ""},
                {"https://handrit.is/oai", "edm", "europeana"},
                {"https://node0-d-efg.d4science.org/efg/mvc/oai/oai.do", "edm", "dfi"},
                {"https://polona.pl/oai/OAIHandler", "oai_dc", "freeAccess"},
                {"https://rep.nlg.gr/oai/request", "edm", "col_20.500.11781_30709"},
                {"http://dspace.bcucluj.ro/oai/request", "oai_dc", "com_123456789_12911"},
                {"http://digital-library.ulbsibiu.ro/oai/request", "oai_dc", "hdl_123456789_384"},
                {"http://gutenberg.beic.it/OAI-PUB", "oai_dc", "oai"},
                {"http://www.doabooks.org/oai", "oai_dc", ""},
                {"http://www.doabooks.org/oai", "oai_dc", ""},
                {"http://www.moldavica.bnrm.md/biblielmo/oaiserver", "oai_dc", "cartipost"},
                {"https://mint-monitor.socialhistoryportal.org/ialhi/oai", "rdf", "1001"},
                {"https://www.rijksmuseum.nl/api2/oai/8zmxuaJ2", "europeana_edm", ""},
                {"http://dare.uva.nl/cgi/arno/oai/oapen", "ese", ""}*/
        };
        return Arrays.asList(data);
    }


    public RealRepositoriesHarvestingTest(String endpoint, String schema, String set) {
        this.endpoint = endpoint;
        this.schema = schema;
        this.set = set;
    }

    @Test
    public void shouldListIdentifiersWithoutSetSpecSpecified() throws Exception {
        Harvest harvest = Harvest.builder()
                .url(endpoint)
                .oaiSetSpec(set != null && set.isEmpty() ?  null : set)
                .metadataPrefix(schema)
                .build();

        System.out.println("Harvesting " + endpoint);

        Harvester harvester = HarvesterFactory.createHarvester(DEFAULT_RETRIES, SLEEP_TIME);
        Iterator<OAIHeader> iterator = harvester.harvestIdentifiers(harvest);
        int counter = 0;

        while(iterator.hasNext()) {
            OAIHeader oaiHeader = iterator.next();
            System.out.println(++counter + " " + oaiHeader.getIdentifier());
        }
        System.out.println(counter + " identifiers harvested");
        //then should not throw any xml parse exception
    }

    @Test
    public void shouldListIdentifiersAndGetRecordsWithoutSetSpecSpecified() throws Exception {
        Harvest harvest = Harvest.builder()
                .url(endpoint)
                .oaiSetSpec(set != null && set.isEmpty() ?  null : set)
                .metadataPrefix(schema)
                .build();

        System.out.println("Harvesting " + endpoint);

        Harvester harvester = HarvesterFactory.createHarvester(DEFAULT_RETRIES, SLEEP_TIME);
        Iterator<OAIHeader> iterator = harvester.harvestIdentifiers(harvest);
        int counter = 0;

        while(iterator.hasNext()) {
            OAIHeader oaiHeader = iterator.next();
            String record = harvestRecord(endpoint, oaiHeader.getIdentifier(), schema);
            System.out.println(++counter + " " + record);
        }
        System.out.println(counter + " identifiers and records harvested");
        //then should not throw any xml parse exception
    }

    @Test
    public void shouldHarvestRecord() throws Exception {
        String record = harvestRecord("http://kobson.nb.rs/oaiphd/oai2.aspx", "oai:doiphd:BG20120531LAZAREVICPASTI", "oai_dc");
        System.out.println(record);
    }

    //this is how we download records, copy-pasted for simplicity
    private String harvestRecord(String oaiPmhEndpoint, String recordId, String metadataPrefix)
            throws OAIRequestException {
        GetRecordParameters params = new GetRecordParameters().withIdentifier(recordId).withMetadataFormatPrefix(metadataPrefix);
        while (true) {
            OaiPmhConnection client = new OaiPmhConnection(oaiPmhEndpoint,
                    org.dspace.xoai.serviceprovider.parameters.Parameters.parameters()
                            .withVerb(Verb.Type.GetRecord).include(params));
            return client.execute();
        }
    }
}
