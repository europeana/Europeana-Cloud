package eu.europeana.cloud.service.dps.storm.topologies.oaipmh;

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

    @Parameters
    public static Collection<Object[]> data() {
        Object[][] data = new Object[][]{
                {"http://islandskort.is/oai", "edm"},
                {"http://baekur.is/oai", "edm"},
                {"http://adl2.kb.dk/oai", "ese"}, //cannot harvest records - GetRecord method doesn't work, identifiers like "/hertz11_lev"
                {"http://panic.image.ece.ntua.gr:9000/photography/oai", "rdf"}, //extremely slow - might not work
                {"http://oai-02.kb.dk/oai/provider", "ese"},
                {"http://oai.kb.dk/oai/provider", "ese"},
        };
        return Arrays.asList(data);
    }


    public RealRepositoriesHarvestingTest(String endpoint, String schema) {
        this.endpoint = endpoint;
        this.schema = schema;
    }

    @Test
    public void shouldListIdentifiersWithoutSetSpecSpecified() throws Exception {

        ListIdentifiersParameters parameters = new ListIdentifiersParameters();
        parameters.withMetadataPrefix(schema);
        System.out.println("Harvesting " + endpoint);
        int i = 0;
        Iterator<Header> headerIterator = sourceProvider.provide(endpoint).listIdentifiers(parameters);
        while (headerIterator.hasNext() && i < 110) { //should go over few pages, as they usually have 50 headers per page
            Header header = headerIterator.next();
        }
        System.out.println(i + " identifiers harvested");
        //then should not throw any xml parse exception
    }

    @Test
    public void shouldListIdentifiersAndGetRecordsWithoutSetSpecSpecified() throws Exception {

        ListIdentifiersParameters parameters = new ListIdentifiersParameters();
        parameters.withMetadataPrefix(schema);
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
