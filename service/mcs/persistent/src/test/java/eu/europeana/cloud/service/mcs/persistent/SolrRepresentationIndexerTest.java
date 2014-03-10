package eu.europeana.cloud.service.mcs.persistent;

import com.google.gson.Gson;
import eu.europeana.cloud.common.web.ParamConstants;
import java.util.HashMap;
import java.util.LinkedHashMap;
import org.junit.After;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(value = { "classpath:/rabbitContext.xml" })
public class SolrRepresentationIndexerTest {

    @Autowired
    SolrRepresentationIndexer indexer;

    @Autowired
    RabbitTemplate template;

    private static final Gson gson = new Gson();


    @After
    public void cleanUp() {
        Mockito.reset(template);
    }


    //TODO - implement
    @Ignore
    @Test
    public void shouldSendMessageAboutNotPersistentRepresentationVersionInsert() {

    }


    @Ignore
    @Test
    public void shouldSendMessageAboutPersistentRepresentationVersionInsert() {

    }


    @Test
    public void shouldSendMessageAboutRepresentationVersionRemoval() {
        //given
        String versionId = "b95fcda0-994a-11e3-bfe1-1c6f653f6012";

        //when
        indexer.removeRepresentationVersion(versionId);
        //then
        verify(template, times(1)).convertAndSend("records.representations.versions.deleteVersion", versionId);
        Mockito.verifyNoMoreInteractions(template);
    }


    @Test
    public void shouldSendMessageAboutRepresentationRemoval() {
        //given
        String cloudId = "fff123123";
        String schemaId = "rdf";
        HashMap<String, String> map = new LinkedHashMap<>();
        map.put(ParamConstants.F_CLOUDID, cloudId);
        map.put(ParamConstants.F_REPRESENTATIONNAME, schemaId);
        String json = gson.toJson(map);

        //when
        indexer.removeRepresentation(cloudId, schemaId);
        //then
        verify(template, times(1)).convertAndSend("records.representations.versions.deleteVersion", json);
        Mockito.verifyNoMoreInteractions(template);
    }
}
