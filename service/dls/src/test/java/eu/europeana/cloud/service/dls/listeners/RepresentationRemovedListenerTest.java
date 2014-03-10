package eu.europeana.cloud.service.dls.listeners;

import com.google.gson.Gson;
import eu.europeana.cloud.common.web.ParamConstants;
import eu.europeana.cloud.service.dls.solr.SolrDAO;
import java.util.HashMap;
import java.util.LinkedHashMap;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(value = { "classpath:/testContext.xml" })
public class RepresentationRemovedListenerTest {

    @Autowired
    RepresentationRemovedListener listener;

    @Autowired
    SolrDAO solrDAO;

    private static final Gson gson = new Gson();


    @After
    public void cleanUp() {
        Mockito.reset(solrDAO);
    }


    @Test
    public void shouldRunRemoveMethodWithRequesedParams()
            throws Exception {
        //given
        HashMap<String, String> map = new LinkedHashMap<>();
        String cloudId = "cloudId";
        String schema = "schema";
        map.put(ParamConstants.F_CLOUDID, cloudId);
        map.put(ParamConstants.F_REPRESENTATIONNAME, schema);
        String body = gson.toJson(map);
        Message message = new Message(body.getBytes(), new MessageProperties());

        //when
        listener.onMessage(message);
        //then
        verify(solrDAO, times(1)).removeRepresentation(cloudId, schema);
        verifyNoMoreInteractions(solrDAO);
    }


    @Test
    public void shouldNotRunRemoveMethodWhenNullParamsSendInMsg()
            throws Exception {
        //given
        HashMap<String, String> map = new LinkedHashMap<>();
        String schema = "schema";
        map.put(ParamConstants.F_REPRESENTATIONNAME, schema);
        String body = gson.toJson(map);
        Message message = new Message(body.getBytes(), new MessageProperties());

        //when
        listener.onMessage(message);
        //then
        verifyZeroInteractions(solrDAO);
    }


    @Test
    public void shouldDoNothingWhenEmptyParamsSendInMsg()
            throws Exception {
        //given
        HashMap<String, String> map = new LinkedHashMap<>();
        String cloudId = "";
        String schema = "schema";
        map.put(ParamConstants.F_CLOUDID, cloudId);
        map.put(ParamConstants.F_REPRESENTATIONNAME, schema);
        String body = gson.toJson(map);
        Message message = new Message(body.getBytes(), new MessageProperties());

        //when
        listener.onMessage(message);
        //then
        verifyZeroInteractions(solrDAO);
    }

}
