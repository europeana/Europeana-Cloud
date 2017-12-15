package eu.europeana.cloud.service.dps.utils.files.counter;

import com.github.tomakehurst.wiremock.client.ResponseDefinitionBuilder;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import com.google.common.collect.Sets;
import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.InputDataType;
import eu.europeana.cloud.service.dps.OAIPMHHarvestingDetails;
import eu.europeana.cloud.service.dps.rest.exceptions.TaskSubmissionException;
import org.apache.commons.io.IOUtils;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static com.github.tomakehurst.wiremock.stubbing.Scenario.STARTED;
import static org.apache.http.HttpHeaders.CONTENT_TYPE;
import static org.apache.http.entity.ContentType.APPLICATION_XML;
import static org.junit.Assert.assertEquals;

public class OaiPmhFilesCounterTest {

    @Rule
    public WireMockRule wireMockRule = new WireMockRule(wireMockConfig().port(9999));

    private static final String OAI_PMH_ENDPOINT = "http://localhost:9999/oai-phm/";

    @Test
    public void shouldGetCorrectCompleteListSize() throws Exception {
        stubFor(get(urlEqualTo("/oai-phm/?verb=ListIdentifiers"))
                .willReturn(response200XmlContent(getFileContent("/oaiListIdentifiers.xml"))));
        OaiPmhFilesCounter counter = new OaiPmhFilesCounter();
        OAIPMHHarvestingDetails details = new OAIPMHHarvestingDetails(null, null, null, null);
        DpsTask task = getDpsTask(details);
        assertEquals(2932, counter.getFilesCount(task, null));
    }

    @Test
    public void shouldReturnMinusOneWhenEmptyCompleteListSize() throws Exception {
        stubFor(get(urlEqualTo("/oai-phm/?verb=ListIdentifiers"))
                .willReturn(response200XmlContent(getFileContent("/oaiListIdentifiersNoCompleteListSize.xml"))));
        OaiPmhFilesCounter counter = new OaiPmhFilesCounter();
        OAIPMHHarvestingDetails details = new OAIPMHHarvestingDetails(null, null, null, null);
        DpsTask task = getDpsTask(details);

        assertEquals(-1, counter.getFilesCount(task, null));
    }

    @Test
    public void shouldReturnMinusOneWhenIncorrectCompleteListSize() throws Exception {
        stubFor(get(urlEqualTo("/oai-phm/?verb=ListIdentifiers"))
                .willReturn(response200XmlContent(getFileContent("/oaiListIdentifiersIncorrectCompleteListSize.xml"))));
        OaiPmhFilesCounter counter = new OaiPmhFilesCounter();
        OAIPMHHarvestingDetails details = new OAIPMHHarvestingDetails(null, null, null, null);
        DpsTask task = getDpsTask(details);

        assertEquals(-1, counter.getFilesCount(task, null));
    }


    @Test
    public void shouldReturnMinusOneWhen200ReturnedButErrorInResponse() throws Exception {
        stubFor(get(urlEqualTo("/oai-phm/?verb=ListIdentifiers"))
                .willReturn(response200XmlContent(getFileContent("/oaiListIdentifiersIncorrectMetadataPrefix.xml"))));
        OaiPmhFilesCounter counter = new OaiPmhFilesCounter();
        OAIPMHHarvestingDetails details = new OAIPMHHarvestingDetails(null, null, null, null);
        DpsTask task = getDpsTask(details);

        assertEquals(-1, counter.getFilesCount(task, null));
    }

    @Test
    public void shouldReturnMinusOneWhenNoResumptionToken() throws Exception {
        stubFor(get(urlEqualTo("/oai-phm/?verb=ListIdentifiers"))
                .willReturn(response200XmlContent(getFileContent("/oaiListIdentifiersNoResumptionToken.xml"))));
        OaiPmhFilesCounter counter = new OaiPmhFilesCounter();
        OAIPMHHarvestingDetails details = new OAIPMHHarvestingDetails(null, null, null, null);
        DpsTask task = getDpsTask(details);

        assertEquals(-1, counter.getFilesCount(task, null));
    }

    @Test(expected = TaskSubmissionException.class)
    public void shouldRetry10TimesAndFail() throws Exception {
        stubFor(get(urlEqualTo("/oai-phm/?verb=ListIdentifiers")).inScenario("Retry and fail scenario")
                .willReturn(response404()));

        OaiPmhFilesCounter counter = new OaiPmhFilesCounter();
        OAIPMHHarvestingDetails details = new OAIPMHHarvestingDetails(null, null, null, null);
        DpsTask task = getDpsTask(details);
        counter.getFilesCount(task, null);
    }

    @Test
    public void shouldRetryAndReturnACorrectValue() throws Exception {
        stubFor(get(urlEqualTo("/oai-phm/?verb=ListIdentifiers")).inScenario("Retry and success scenario").whenScenarioStateIs(STARTED).willSetStateTo("one time requested")
                .willReturn(response404()));
        stubFor(get(urlEqualTo("/oai-phm/?verb=ListIdentifiers")).inScenario("Retry and success scenario").whenScenarioStateIs("one time requested")
                .willReturn(response200XmlContent(getFileContent("/oaiListIdentifiers.xml"))));
        OaiPmhFilesCounter counter = new OaiPmhFilesCounter();
        OAIPMHHarvestingDetails details = new OAIPMHHarvestingDetails(null, null, null, null);
        DpsTask task = getDpsTask(details);
        assertEquals(2932, counter.getFilesCount(task, null));
    }

    @Test
    public void shouldReturnCorrectSumWhenSchemasAndSetListed() throws Exception {
        String schema1 = "schema1";
        String schema2 = "schema2";

        String set1 = "set1";

        stubFor(get(urlEqualTo("/oai-phm/?verb=ListIdentifiers&set=" + set1 + "&metadataPrefix=" + schema1))
                .willReturn(response200XmlContent(getFileContent("/oaiListIdentifiers.xml"))));
        stubFor(get(urlEqualTo("/oai-phm/?verb=ListIdentifiers&set=" + set1 + "&metadataPrefix=" + schema2))
                .willReturn(response200XmlContent(getFileContent("/oaiListIdentifiers2.xml"))));

        OaiPmhFilesCounter counter = new OaiPmhFilesCounter();
        OAIPMHHarvestingDetails details = new OAIPMHHarvestingDetails(Sets.newHashSet(schema1, schema2), Sets.newHashSet(set1), null, null);
        DpsTask task = getDpsTask(details);

        assertEquals(2934, counter.getFilesCount(task, null));
    }

    @Test
    public void shouldReturnCorrectSumWhenSchemasAndNoSetsListed() throws Exception {
        String schema1 = "schema1";
        String schema2 = "schema2";

        stubFor(get(urlEqualTo("/oai-phm/?verb=ListIdentifiers&metadataPrefix=" + schema1))
                .willReturn(response200XmlContent(getFileContent("/oaiListIdentifiers.xml"))));
        stubFor(get(urlEqualTo("/oai-phm/?verb=ListIdentifiers&metadataPrefix=" + schema2))
                .willReturn(response200XmlContent(getFileContent("/oaiListIdentifiers2.xml"))));


        OaiPmhFilesCounter counter = new OaiPmhFilesCounter();
        OAIPMHHarvestingDetails details = new OAIPMHHarvestingDetails(Sets.newHashSet(schema1, schema2), null, null, null);
        DpsTask task = getDpsTask(details);

        assertEquals(2934, counter.getFilesCount(task, null));
    }

    @Test
    public void shouldReturnMinusOneWhenMultipleSetsSpecified() throws Exception {
        OaiPmhFilesCounter counter = new OaiPmhFilesCounter();
        OAIPMHHarvestingDetails details = new OAIPMHHarvestingDetails(null, null, Sets.newHashSet("a", "b", "c"), null, null, null);
        DpsTask task = getDpsTask(details);

        assertEquals(-1, counter.getFilesCount(task, null));
    }


    @Test
    public void shouldReturnCountForSchemaWhenEmptySetsProvided() throws Exception {
        stubFor(get(urlEqualTo("/oai-phm/?verb=ListIdentifiers"))
                .willReturn(response200XmlContent(getFileContent("/oaiListIdentifiers.xml"))));
        OaiPmhFilesCounter counter = new OaiPmhFilesCounter();
        OAIPMHHarvestingDetails details = new OAIPMHHarvestingDetails(null, null, new HashSet<String>(), null, null, null);
        DpsTask task = getDpsTask(details);

        assertEquals(2932, counter.getFilesCount(task, null));
    }

    @Test
    public void shouldReturnMinusOneWhenSetsExcluded() throws Exception {
        OaiPmhFilesCounter counter = new OaiPmhFilesCounter();
        OAIPMHHarvestingDetails details = new OAIPMHHarvestingDetails(null, null, null, Sets.newHashSet("a", "b", "c"), null, null);
        DpsTask task = getDpsTask(details);

        assertEquals(-1, counter.getFilesCount(task, null));
    }

    @Test
    public void shouldReturnMinusOneWheHarvestingDetailsIsNotProvided() throws Exception {
        OaiPmhFilesCounter counter = new OaiPmhFilesCounter();
        DpsTask task = getDpsTask(null);
        assertEquals(-1, counter.getFilesCount(task, null));
    }

    @Test
    public void shouldReturnMinusOneWhenSchemasExcluded() throws Exception {
        OaiPmhFilesCounter counter = new OaiPmhFilesCounter();
        OAIPMHHarvestingDetails details = new OAIPMHHarvestingDetails(null, Sets.newHashSet("a", "b", "c"), null, null, null, null);
        DpsTask task = getDpsTask(details);

        assertEquals(-1, counter.getFilesCount(task, null));
    }

    @Test(expected = TaskSubmissionException.class)
    public void shouldThrowTaskSubmissionExceptionWhenURLsIsNull() throws Exception {
        OaiPmhFilesCounter counter = new OaiPmhFilesCounter();
        OAIPMHHarvestingDetails details = new OAIPMHHarvestingDetails(null, null, null, null, null, null);
        DpsTask task = getDpsTaskNoEndpoint(details);
        counter.getFilesCount(task, null);
    }

    private DpsTask getDpsTask(OAIPMHHarvestingDetails details) {
        DpsTask task = getDpsTaskNoEndpoint(details);

        List<String> urls = new ArrayList<>();
        urls.add(OAI_PMH_ENDPOINT);
        task.addDataEntry(InputDataType.REPOSITORY_URLS, urls);

        return task;
    }

    private DpsTask getDpsTaskNoEndpoint(OAIPMHHarvestingDetails details) {
        DpsTask task = new DpsTask("test_oai_task");
        task.setHarvestingDetails(details);
        return task;
    }


    private ResponseDefinitionBuilder response200XmlContent(String fileContent) {
        return aResponse()
                .withHeader(CONTENT_TYPE, APPLICATION_XML.getMimeType())
                .withStatus(200)
                .withBody(fileContent);
    }

    private ResponseDefinitionBuilder response404() {
        return aResponse()
                .withHeader(CONTENT_TYPE, APPLICATION_XML.getMimeType())
                .withStatus(404);
    }

    private String getFileContent(String name) throws IOException {
        return IOUtils.toString(
                Object.class.getResourceAsStream(name),
                "UTF-8");
    }
}