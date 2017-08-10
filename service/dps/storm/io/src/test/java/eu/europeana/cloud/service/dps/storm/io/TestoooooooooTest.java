package eu.europeana.cloud.service.dps.storm.io;

import eu.europeana.cloud.client.uis.rest.CloudException;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * @author krystian.
 */
public class TestoooooooooTest {

    @Test
    public void shouldSaveFile() throws CloudException {
        //given
        final OAIWriteRecordBolt oaiWriteRecordBolt = new OAIWriteRecordBolt("http://localhost:8080/mcs", "http://localhost:8080/uis");


        //when
        final String result = oaiWriteRecordBolt.getCloudId("Basic YWRtaW46ZWNsb3VkX2FkbWlu", "admin", "oai:mediateka" +
                ".centrumzamenhofa.pl:206");

        //then
        assertThat(result,is(""));
    }
}
