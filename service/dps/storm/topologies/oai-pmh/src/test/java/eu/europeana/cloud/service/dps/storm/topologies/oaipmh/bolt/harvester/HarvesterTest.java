package eu.europeana.cloud.service.dps.storm.topologies.oaipmh.bolt.harvester;

import eu.europeana.cloud.service.dps.storm.topologies.oaipmh.exceptions.HarvesterException;
import eu.europeana.cloud.service.dps.storm.topologies.oaipmh.helper.WiremockHelper;
import org.dspace.xoai.serviceprovider.exceptions.OAIRequestException;
import org.junit.Before;
import org.junit.Test;

import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathFactory;
import java.io.IOException;
import java.io.InputStream;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static eu.europeana.cloud.service.dps.storm.topologies.oaipmh.helper.TestHelper.convertToString;
import static eu.europeana.cloud.service.dps.storm.topologies.oaipmh.helper.TestHelper.isSimilarXml;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * @author krystian.
 */
public class HarvesterTest extends WiremockHelper {

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
        final Harvester harvester = new Harvester();

        //when
        final InputStream result = harvester.harvestRecord(OAI_PMH_ENDPOINT, "mediateka",
                "oai_dc", expr, isDeletedExpression);

        //then
        final String actual = convertToString(result);
        assertThat(actual, isSimilarXml(getFileContent("/expectedOaiRecord.xml")));
    }


    @Test(expected = HarvesterException.class)
    public void shouldHandleDeletedRecords() throws IOException, HarvesterException {
        //given
        stubFor(get(urlEqualTo("/oai-phm/?verb=GetRecord&identifier=mediateka" +
                "&metadataPrefix=oai_dc"))
                .willReturn(response200XmlContent(getFileContent("/deletedOaiRecord.xml"))
                ));
        final Harvester harvester = new Harvester();

        //when
        harvester.harvestRecord(OAI_PMH_ENDPOINT, "mediateka",
                "oai_dc", expr, isDeletedExpression);

    }

    @Test
    public void shouldThrowExceptionHarvestedRecordNotFound() throws OAIRequestException, IOException,
            HarvesterException {
        //given
        stubFor(get(urlEqualTo("/oai-phm/?verb=GetRecord&identifier=oai%3Amediateka.centrumzamenhofa" +
                ".pl%3A19&metadataPrefix=oai_dc"))
                .willReturn(response404()));
        final Harvester harvester = new Harvester();

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
                .willReturn(responsTimeoutMoreThanSocketTimeout(getFileContent("/sampleOaiRecord.xml"))
                ));
        final Harvester harvester = new Harvester();

        //when
        final InputStream result = harvester.harvestRecord(OAI_PMH_ENDPOINT, "mediateka",
                "oai_dc", expr, isDeletedExpression);

        //then
        //exception expected
    }


}