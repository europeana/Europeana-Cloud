package eu.europeana.cloud.service.dps.storm.topologies.oaipmh.common;

import org.dspace.xoai.model.oaipmh.MetadataFormat;
import eu.europeana.cloud.service.dps.storm.topologies.oaipmh.helper.WiremockHelper;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static org.hamcrest.core.Is.is;
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


}