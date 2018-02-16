package eu.europeana.cloud.service.dps.storm.topologies.oaipmh;

import eu.europeana.cloud.service.dps.storm.topologies.oaipmh.common.OAIHelper;
import eu.europeana.cloud.service.dps.storm.topologies.oaipmh.helpers.SourceProvider;
import org.apache.commons.io.IOUtils;
import org.dspace.xoai.model.oaipmh.Header;
import org.dspace.xoai.model.oaipmh.Verb;
import org.dspace.xoai.serviceprovider.client.HttpOAIClient;
import org.dspace.xoai.serviceprovider.client.OAIClient;
import org.dspace.xoai.serviceprovider.exceptions.OAIRequestException;
import org.dspace.xoai.serviceprovider.parameters.GetRecordParameters;
import org.dspace.xoai.serviceprovider.parameters.ListIdentifiersParameters;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.io.InputStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.isEmptyString;
import static org.hamcrest.core.IsNot.not;

@Ignore
@RunWith(Parameterized.class)
public class RealRepositoriesHarvestingTest {

    private final SourceProvider sourceProvider = new SourceProvider();
    private final String endpoint;
    private final String schema;
    private final String set;

    @Parameters
    public static Collection<Object[]> data() {
        Object[][] data = new Object[][]{
                {"http://islandskort.is/oai", "edm",""},
                {"http://baekur.is/oai", "edm",""},
                {"http://adl2.kb.dk/oai", "ese",""}, //cannot harvest records - GetRecord method doesn't work, identifiers like "/hertz11_lev"
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
                {"http://diglit.ub.uni-heidelberg.de/cgi-bin/digioai.cgi", "mets","druckschriften"},
                {"http://dlib.mk/oai/request", "oai_dc","com_68275_66"},
                {"http://europeana.cbuc.cat/oai.php", "ese","manuscritAB"},
                {"http://formula1.csc.fi/oai-pmh", "edm","musketti"},
                {"http://haw.nsk.hr/oai/", "oai_dc",""},
                {"http://kobson.nb.rs/oaiphd/oai2.aspx", "oai_dc","phd"},
                {"http://more.locloud.eu:8080/carare", "EDM","SNHB"},
                {"http://more.locloud.eu:8080/locloud", "EDM","IPCHS"},
                {"http://museion.esbirky.cz/w4m/oai/MRK", "ese","SMUH"},
                {"http://nukoai.nuk.uni-lj.si:8089/repox/OAIHandler", "edm","Rise_of_Literacy_in_Europe_part_1"},
                {"http://oai-bdb.onb.ac.at/Script/oai2.aspx", "ese","9846792"},
                {"http://oaipmh.lnb.lv/repox/OAIHandler", "ese","zlpics"},
                {"http://repox.kamra.si:8080/repox/OAIHandler", "edm","Europeana_Kamra_Multimedia"},
                {"http://stary.webumenia.sk/oai-pmh", "ese","Europeana SNG"},
                {"http://test117.ait.co.at/oai-provider-edm/oai/", "edm","MNHN"},
                {"http://timarit.is/oai", "edm","publications"},
                {"http://www.kulturpool.at/rest/export/1.0/oai-pmh", "edm","oeaw"},
                {"http://www.manuscriptorium.com/oai/", "ese","digitized"},
                {"https://aggregator.ekt.gr/aggregator-oai/request", "edm","Digital_Pylia"},
                {"https://cdk.lib.cas.cz/oai", "ese",""},
                {"https://dr.nsk.hr/oai", "oai_dc",""},
                {"https://handrit.is/oai", "edm","europeana"},
                {"https://node0-d-efg.d4science.org/efg/mvc/oai/oai.do","edm","dfi"},
                {"https://polona.pl/oai/OAIHandler","oai_dc","freeAccess"},
                {"https://rep.nlg.gr/oai/request","edm","col_20.500.11781_30709"},
                {"http://dspace.bcucluj.ro/oai/request","oai_dc","com_123456789_12911"},
                {"http://digital-library.ulbsibiu.ro/oai/request","oai_dc","hdl_123456789_384"},
                {"http://gutenberg.beic.it/OAI-PUB","oai_dc","oai"},
                {"http://www.doabooks.org/oai","oai_dc",""},
                {"http://www.doabooks.org/oai","oai_dc",""},
                {"http://www.moldavica.bnrm.md/biblielmo/oaiserver","oai_dc","cartipost"},
                {"https://mint-monitor.socialhistoryportal.org/ialhi/oai","rdf","1001"},
                {"https://www.rijksmuseum.nl/api2/oai/8zmxuaJ2","europeana_edm",""},
                {"http://dare.uva.nl/cgi/arno/oai/oapen","ese",""}
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

        ListIdentifiersParameters parameters = new ListIdentifiersParameters();
        parameters.withMetadataPrefix(schema);
        if(!set.isEmpty())
            parameters.withSetSpec(set);
        System.out.println("Harvesting " + endpoint);
        int i = 0;
        Iterator<Header> headerIterator = sourceProvider.provide(endpoint).listIdentifiers(parameters);
        while (headerIterator.hasNext() ) { //should go over few pages, as they usually have 50 headers per page
            Header header = headerIterator.next();
            System.out.println(i + " " + header.getIdentifier());
            ++i;
        }
        System.out.println(i + " identifiers harvested");
        //then should not throw any xml parse exception
    }

    @Test
    public void shouldListIdentifiersAndGetRecordsWithoutSetSpecSpecified() throws Exception {

        ListIdentifiersParameters parameters = new ListIdentifiersParameters();
        parameters.withMetadataPrefix(schema);
        if(!set.isEmpty())
            parameters.withSetSpec(set);
        System.out.println("Harvesting " + endpoint);
        int i = 0;
        Iterator<Header> headerIterator = sourceProvider.provide(endpoint).listIdentifiers(parameters);
        while (headerIterator.hasNext() && i < 110) { //should go over few pages, as they usually have 50 headers per page
            Header header = headerIterator.next();
            InputStream inputStream = harvestRecord(endpoint, header.getIdentifier(), schema);
            String record = IOUtils.toString(inputStream, "UTF-8");
            assertThat(record, not(isEmptyString()));
            System.out.println(record);
            IOUtils.closeQuietly(inputStream);
            i++;
        }
        System.out.println(i + " records harvested");
        //then should not throw any xml parse exception
    }

    @Test
    public void shouldHarvestRecord() throws Exception{
        InputStream inputStream = harvestRecord("http://kobson.nb.rs/oaiphd/oai2.aspx","oai:doiphd:BG20120531LAZAREVICPASTI","oai_dc");
        String record = IOUtils.toString(inputStream, "UTF-8");
        assertThat(record, not(isEmptyString()));
        System.out.println(record);
        IOUtils.closeQuietly(inputStream);
    }


    @Test
    public void shouldReadGranularity(){
        OAIHelper helper = new OAIHelper(endpoint);
        helper.getGranularity();
    }

    //this is how we download records, copy-pasted for simplicity
    private InputStream harvestRecord(String oaiPmhEndpoint, String recordId, String metadataPrefix)
            throws OAIRequestException {
        GetRecordParameters params = new GetRecordParameters().withIdentifier(recordId).withMetadataFormatPrefix(metadataPrefix);
        while (true) {
            OAIClient client = new HttpOAIClient(oaiPmhEndpoint);
            return client.execute(org.dspace.xoai.serviceprovider.parameters.Parameters.parameters().withVerb(Verb.Type.GetRecord).include(params));
        }
    }

}



