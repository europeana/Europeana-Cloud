package eu.europeana.cloud.service.dps.utils.files.counter;

import com.github.tomakehurst.wiremock.client.ResponseDefinitionBuilder;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import com.google.common.collect.Sets;
import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.OAIPMHHarvestingDetails;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import org.apache.commons.io.IOUtils;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.util.HashSet;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
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
    public void shouldReturnMinusOneWhen404Returned() throws Exception {
        stubFor(get(urlEqualTo("/oai-phm/?verb=ListIdentifiers"))
                .willReturn(response404()));
        OaiPmhFilesCounter counter = new OaiPmhFilesCounter();
        OAIPMHHarvestingDetails details = new OAIPMHHarvestingDetails(null, null, null, null);
        DpsTask task = getDpsTask(details);
        assertEquals(-1, counter.getFilesCount(task, null));
    }

    @Test
    public void shouldReturnCorrectSumWhenSchemasAndSetListed() throws Exception {
        String schema1 = "schema1";
        String schema2 = "schema2";

        String set1 = "set1";

        stubFor(get(urlEqualTo("/oai-phm/?verb=ListIdentifiers&set=" + set1 + "&metadataPrefix=" + schema1 ))
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

        stubFor(get(urlEqualTo("/oai-phm/?verb=ListIdentifiers&metadataPrefix=" + schema1 ))
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
    public void shouldReturnMinusOneWhenEmptySchemasProvided() throws Exception {
        OaiPmhFilesCounter counter = new OaiPmhFilesCounter();
        OAIPMHHarvestingDetails details = new OAIPMHHarvestingDetails(null, null, new HashSet<String>(), null, null, null);
        DpsTask task = getDpsTask(details);

        assertEquals(-1, counter.getFilesCount(task, null));
    }


    @Test
    public void shouldReturnMinusOneWhenSetsExcluded() throws Exception {
        OaiPmhFilesCounter counter = new OaiPmhFilesCounter();
        OAIPMHHarvestingDetails details = new OAIPMHHarvestingDetails(null, null, null, Sets.newHashSet("a", "b", "c"), null, null);
        DpsTask task = getDpsTask(details);

        assertEquals(-1, counter.getFilesCount(task, null));
    }

    @Test
    public void shouldReturnMinusOneWhenSchemasExcluded() throws Exception {
        OaiPmhFilesCounter counter = new OaiPmhFilesCounter();
        OAIPMHHarvestingDetails details = new OAIPMHHarvestingDetails(null,Sets.newHashSet("a", "b", "c"),  null, null, null, null);
        DpsTask task = getDpsTask(details);

        assertEquals(-1, counter.getFilesCount(task, null));
    }

    private DpsTask getDpsTask(OAIPMHHarvestingDetails details) {
        DpsTask task = new DpsTask("test_oai_task");
        task.setHarvestingDetails(details);
        task.addParameter(PluginParameterKeys.DPS_TASK_INPUT_DATA, OAI_PMH_ENDPOINT);
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