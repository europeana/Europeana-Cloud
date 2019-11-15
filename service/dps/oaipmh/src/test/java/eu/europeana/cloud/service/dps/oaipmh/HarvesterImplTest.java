package eu.europeana.cloud.service.dps.oaipmh;

import eu.europeana.cloud.service.dps.oaipmh.HarvesterImpl.ConnectionFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import org.dspace.xoai.model.oaipmh.MetadataFormat;
import org.dspace.xoai.serviceprovider.parameters.Parameters;
import org.hamcrest.core.Is;
import org.junit.Before;
import org.junit.Test;

import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathFactory;
import java.io.IOException;
import java.io.InputStream;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;
import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * @author krystian.
 */
public class HarvesterImplTest extends WiremockHelper {

    private static final String OAI_PMH_ENDPOINT = "http://localhost:8181/oai-phm/";
    private static final String METADATA_XPATH = "/*[local-name()='OAI-PMH']" +
            "/*[local-name()='GetRecord']" +
            "/*[local-name()='record']" +
            "/*[local-name()='metadata']" +
            "/child::*";

    private static final String IS_DELETED_XPATH = "string(/*[local-name()='OAI-PMH']" +
            "/*[local-name()='GetRecord']" +
            "/*[local-name()='record']" +
            "/*[local-name()='header']" +
            "/@status)";

    private static final int TEST_SOCKET_TIMEOUT = 10 * 1000; /* = 10sec */

    private static final String EDM = "EDM";
    private static final String RDF = "RDF";
    private static final String OAI_DC = "OAI_DC";

    private XPathExpression isDeletedExpression;
    private XPathExpression expr;

    @Before
    public void init() throws Exception {
        XPath xpath = XPathFactory.newInstance().newXPath();
        expr = xpath.compile(METADATA_XPATH);
        isDeletedExpression = xpath.compile(IS_DELETED_XPATH);
    }

    @Test
    public void shouldHarvestRecord() throws IOException, HarvesterException {
        //given
        stubFor(get(urlEqualTo("/oai-phm/?verb=GetRecord&identifier=mediateka" +
                "&metadataPrefix=oai_dc"))
                .willReturn(response200XmlContent(getFileContent("/sampleOaiRecord.xml"))
                ));
        final HarvesterImpl harvester = new HarvesterImpl(DEFAULT_RETRIES, SLEEP_TIME);

        //when
        final InputStream result = harvester.harvestRecord(OAI_PMH_ENDPOINT, "mediateka",
                "oai_dc", expr, isDeletedExpression);

        //then
        final String actual = TestHelper.convertToString(result);
        assertThat(actual, TestHelper.isSimilarXml(getFileContent("/expectedOaiRecord.xml")));
    }


    @Test(expected = HarvesterException.class)
    public void shouldHandleDeletedRecords() throws IOException, HarvesterException {
        //given
        stubFor(get(urlEqualTo("/oai-phm/?verb=GetRecord&identifier=mediateka" +
                "&metadataPrefix=oai_dc"))
                .willReturn(response200XmlContent(getFileContent("/deletedOaiRecord.xml"))
                ));
        final HarvesterImpl harvester = new HarvesterImpl(DEFAULT_RETRIES, SLEEP_TIME);

        //when
        harvester.harvestRecord(OAI_PMH_ENDPOINT, "mediateka",
                "oai_dc", expr, isDeletedExpression);

    }

    @Test
    public void shouldThrowExceptionHarvestedRecordNotFound() {
        //given
        stubFor(get(urlEqualTo("/oai-phm/?verb=GetRecord&identifier=oai%3Amediateka.centrumzamenhofa" +
                ".pl%3A19&metadataPrefix=oai_dc"))
                .willReturn(response404()));
        final HarvesterImpl harvester = new HarvesterImpl(DEFAULT_RETRIES, SLEEP_TIME);

        //when
        try {
            harvester.harvestRecord(OAI_PMH_ENDPOINT, "oai:mediateka.centrumzamenhofa.pl:19",
                    "oai_dc", expr, isDeletedExpression);
            fail();
        } catch (HarvesterException e) {
            //then
            assertThat(e.getMessage(), is("Problem with harvesting record oai:mediateka.centrumzamenhofa.pl:19 for endpoint http://localhost:8181/oai-phm/ because of: Error querying service. Returned HTTP Status Code: 404"));
        }
    }

    @Test(expected = HarvesterException.class)
    public void shouldHandleTimeout() throws IOException, HarvesterException {
        //given
        stubFor(get(urlEqualTo("/oai-phm/?verb=GetRecord&identifier=mediateka" +
                "&metadataPrefix=oai_dc"))
                .willReturn(responsTimeoutGreaterThanSocketTimeout(getFileContent("/sampleOaiRecord.xml"), TEST_SOCKET_TIMEOUT)
                ));
        final ConnectionFactory connectionFactory = new ConnectionFactory() {
            @Override
            public OaiPmhConnection createConnection(String oaiPmhEndpoint, Parameters parameters) {
                return new OaiPmhConnection(oaiPmhEndpoint, parameters, TEST_SOCKET_TIMEOUT, TEST_SOCKET_TIMEOUT, TEST_SOCKET_TIMEOUT);
            }
        };
        final HarvesterImpl harvester = new HarvesterImpl(1, SLEEP_TIME);

        //when
        harvester.harvestRecord(connectionFactory, OAI_PMH_ENDPOINT, "mediateka", "oai_dc", expr,
                isDeletedExpression);

        //then
        //exception expected
    }

    @Test
    public void testGetSchemas() throws HarvesterException {

        // Prepare
        final String fileUrl = "file url";
        final HarvesterImpl harvester = spy(new HarvesterImpl(3, 1000));

        final MetadataFormat metadataFormat1 = new MetadataFormat();
        metadataFormat1.withMetadataPrefix(EDM);

        final MetadataFormat metadataFormat2 = new MetadataFormat();
        metadataFormat2.withMetadataPrefix(RDF);

        final MetadataFormat metadataFormat3 = new MetadataFormat();
        metadataFormat3.withMetadataPrefix(OAI_DC);

        // Create new iterator every time: otherwise we'd need to reset the iterator.
        doAnswer(new Answer<Iterator<MetadataFormat>>() {
            @Override
            public Iterator<MetadataFormat> answer(InvocationOnMock invocationOnMock) {
                return Arrays.asList(metadataFormat1, metadataFormat2, metadataFormat3).iterator();
            }
        }).when(harvester).getSchemaIterator(fileUrl);

        // Test without exclusions
        final Set<String> schemasWithNullExclusions = harvester.getSchemas(fileUrl, null);
        assertEquals(new HashSet<>(Arrays.asList(EDM, RDF, OAI_DC)), schemasWithNullExclusions);
        final Set<String> schemasWithEmptyExclusions = harvester.getSchemas(fileUrl,
                Collections.<String>emptySet());
        assertEquals(new HashSet<>(Arrays.asList(EDM, RDF, OAI_DC)), schemasWithEmptyExclusions);

        // Test with exclusions
        final Set<String> schemasWithSomeExclusions = harvester.getSchemas(fileUrl,
                new HashSet<>(Arrays.asList(OAI_DC, RDF)));
        assertEquals(Collections.singleton(EDM), schemasWithSomeExclusions);
        final Set<String> schemasWithAllExclusions = harvester.getSchemas(fileUrl,
                new HashSet<>(Arrays.asList(EDM, OAI_DC, RDF)));
        assertEquals(Collections.emptySet(), schemasWithAllExclusions);
    }

    @Test
    public void testGetSchemaIterator() throws IOException, HarvesterException {
        //given
        stubFor(get(urlEqualTo("/oai-phm/?verb=ListMetadataFormats"))
                .willReturn(response200XmlContent(getFileContent("/schemas.xml"))
                ));
        final HarvesterImpl harvester = new HarvesterImpl(DEFAULT_RETRIES, SLEEP_TIME);

        //when
        final Iterator<MetadataFormat> result = harvester.getSchemaIterator(OAI_PMH_ENDPOINT);

        //then
        List<MetadataFormat> resultAsList = convertToList(result);
        assertThat(resultAsList.size(), Is.is(1));
        assertThat(resultAsList.get(0).getMetadataPrefix(), Is.is("oai_dc"));
    }

    private List<MetadataFormat> convertToList(Iterator<MetadataFormat> metadataFormatIterator) {
        List<MetadataFormat> result = new ArrayList<>();
        while (metadataFormatIterator.hasNext()){
            result.add(metadataFormatIterator.next());
        }
        return result;
    }
}
