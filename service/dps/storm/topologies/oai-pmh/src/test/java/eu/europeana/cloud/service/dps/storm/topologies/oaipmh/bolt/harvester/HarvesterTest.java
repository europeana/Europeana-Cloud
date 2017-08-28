package eu.europeana.cloud.service.dps.storm.topologies.oaipmh.bolt.harvester;

import com.lyncode.xoai.serviceprovider.exceptions.OAIRequestException;
import eu.europeana.cloud.service.dps.storm.topologies.oaipmh.exceptions.HarvesterException;
import eu.europeana.cloud.service.dps.storm.topologies.oaipmh.helper.WiremockHelper;
import org.junit.Test;

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

    @Test
    public void shouldHarvestRecord() throws IOException, HarvesterException {
        //given
        stubFor(get(urlEqualTo("/oai-phm/?verb=GetRecord&identifier=oai%3Amediateka.centrumzamenhofa.pl%3A19&metadataPrefix=oai_dc"))
                .willReturn(response200XmlContent(getFileContent("/sampleOaiRecord.xml"))
         ));
        final Harvester harvester = new Harvester();

        //when
        final InputStream result = harvester.harvestRecord("http://www.mediateka.centrumzamenhofa.pl/oai-phm/","oai:mediateka" +
                        ".centrumzamenhofa.pl:19",
                "oai_dc");

        //then
        final String actual = convertToString(result);
        assertThat(actual, isSimilarXml(getFileContent("/expectedOaiRecord.xml")));
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
            harvester.harvestRecord(OAI_PMH_ENDPOINT,"oai:mediateka.centrumzamenhofa.pl:19",
                    "oai_dc");
            fail();
        }catch (HarvesterException e){
            //then
            assertThat(e.getMessage(),is("Problem with harvesting record oai:mediateka.centrumzamenhofa.pl:19 for endpoint http://localhost:8181/oai-phm/"));
        }
    }



}