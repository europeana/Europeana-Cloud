package eu.europeana.cloud.service.dps.storm.topologies.oaipmh.common;

import com.lyncode.xoai.model.oaipmh.Granularity;
import com.lyncode.xoai.model.oaipmh.MetadataFormat;
import com.lyncode.xoai.serviceprovider.exceptions.InvalidOAIResponse;
import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.OAIPMHHarvestingDetails;
import eu.europeana.cloud.service.dps.storm.topologies.oaipmh.helper.WiremockHelper;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static com.github.tomakehurst.wiremock.stubbing.Scenario.STARTED;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

/**
 * @author krystian.
 */
public class OAIHelperTest extends WiremockHelper {
    @Test
    public void shouldFetchSchemas() throws IOException {
        //given
        stubFor(get(urlEqualTo("/oai-phm/?verb=ListMetadataFormats"))
                .willReturn(response200XmlContent(getFileContent("/schemas.xml"))
                ));
        final OAIHelper underTest = new OAIHelper("http://localhost:8181/oai-phm/");

        //when
        final Iterator<MetadataFormat> result = underTest.listSchemas();

        //then
        List<MetadataFormat> resultAsList = convertToList(result);
        assertThat(resultAsList.size(),is(1));
        assertThat(resultAsList.get(0).getMetadataPrefix(),is("oai_dc"));

    }

    private List<MetadataFormat> convertToList(Iterator<MetadataFormat> metadataFormatIterator) {
        List<MetadataFormat> result = new ArrayList<>();
        while (metadataFormatIterator.hasNext()){
            result.add(metadataFormatIterator.next());
        }
        return result;
    }

    @Test
    public void shouldRetrieveDayGranularity() throws IOException {
        //given
        stubFor(get(urlEqualTo("/oai-phm/?verb=Identify")).willReturn(response200XmlContent(getFileContent("/identifyDayGranularity.xml"))));
        final OAIHelper underTest = new OAIHelper("http://localhost:8181/oai-phm/");
        //when
        Granularity result = underTest.getGranularity();
        //then
        assertThat(result, is(Granularity.Day));
    }

    @Test
    public void shouldRetrieveSecondGranularity() throws IOException {
        //given
        stubFor(get(urlEqualTo("/oai-phm/?verb=Identify")).willReturn(response200XmlContent(getFileContent("/identifySecondGranularity.xml"))));
        final OAIHelper underTest = new OAIHelper("http://localhost:8181/oai-phm/");
        //when
        Granularity result = underTest.getGranularity();
        //then
        assertThat(result, is(Granularity.Second));
    }

    @Test(expected = NoSuchElementException.class)
    public void shouldThrowIllegalArgumentExceptionWhenNoGranularityAvailable() throws IOException {
        //given
        stubFor(get(urlEqualTo("/oai-phm/?verb=Identify")).willReturn(response200XmlContent(getFileContent("/identifyNoGranularity.xml"))));
        final OAIHelper underTest = new OAIHelper("http://localhost:8181/oai-phm/");
        //when
        Granularity result = underTest.getGranularity();
        //then exception is thrown
    }



    @Test(expected = InvalidOAIResponse.class)
    public void shouldRetry10TimesAndFail() throws InvalidOAIResponse {
        for (int i = 0; i < 10; i++)
            stubFor(get(urlEqualTo("/oai-phm/?verb=Identify")).inScenario("Retry and fail scenario")
                    .willReturn(response404()));

        final OAIHelper underTest = new OAIHelper("http://localhost:8181/oai-phm/");
        //when
        Granularity result = underTest.getGranularity();
        //then
        //exception is thrown
    }

    @Test
    public void shouldRetryAndSucceed() throws Exception {
        //given
        stubFor(get(urlEqualTo("/oai-phm/?verb=Identify")).inScenario("Retry and success scenario")
                .whenScenarioStateIs(STARTED).willSetStateTo("one time requested")
                .willReturn(response404()));
        stubFor(get(urlEqualTo("/oai-phm/?verb=Identify")).inScenario("Retry and success scenario")
                .whenScenarioStateIs("one time requested")
                .willReturn(response200XmlContent(getFileContent("/identifySecondGranularity.xml"))));

        final OAIHelper underTest = new OAIHelper("http://localhost:8181/oai-phm/");
        //when
        Granularity result = underTest.getGranularity();
        //then
        assertThat(result, is(Granularity.Second));
    }

    @Test
    public void listSchemas() {
    }

    @Test
    public void getEarlierDate() {
    }
}