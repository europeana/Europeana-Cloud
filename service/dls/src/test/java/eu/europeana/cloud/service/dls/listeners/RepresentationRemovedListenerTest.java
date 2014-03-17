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
@ContextConfiguration(value = {"classpath:/testContext.xml"})
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
    public void shouldCallRemoveRepresentation()
            throws Exception {
        //given
        String cloudId = "cloudId123";
        String representationName = "representation123";
        Message message = new Message(prepareRemoveRepresentationMessage(cloudId, representationName).getBytes(), new MessageProperties());
        //when
        listener.onMessage(message);
        //then
        verify(solrDAO, times(1)).removeRepresentation(cloudId, representationName);
        verifyNoMoreInteractions(solrDAO);
    }

    @Test
    public void shouldNotCallDAOWhenReceivedMessageWithNullBody()
            throws Exception {
        //given
        Message message = new Message(null, new MessageProperties());
        //when
        listener.onMessage(message);
        //then
        verifyZeroInteractions(solrDAO);
    }

    @Test
    public void shouldNotCallDAOWhenReceivedEmptyMessage()
            throws Exception {
        //given
        Message message = new Message("".getBytes(), new MessageProperties());
        //when
        listener.onMessage(message);
        //then
        verifyZeroInteractions(solrDAO);
    }

    @Test
    public void shouldNotCallDAOWhenReceivedNullCloudId()
            throws Exception {
        //given
        String cloudId = null;
        String representationName = "representation123";
        Message message = new Message(prepareRemoveRepresentationMessage(cloudId, representationName).getBytes(), new MessageProperties());
        //when
        listener.onMessage(message);
        //then
        verifyZeroInteractions(solrDAO);
    }

    @Test
    public void shouldNotCallDAOWhenReceivedEmptyCloudId()
            throws Exception {
        //given
        String cloudId = "";
        String representationName = "representation123";
        Message message = new Message(prepareRemoveRepresentationMessage(cloudId, representationName).getBytes(), new MessageProperties());
        //when
        listener.onMessage(message);
        //then
        verifyZeroInteractions(solrDAO);
    }

    @Test
    public void shouldNotCallDAOWhenReceivedNullRepresentationName()
            throws Exception {
        //given
        String cloudId = "cloudId123";
        String representationName = null;
        Message message = new Message(prepareRemoveRepresentationMessage(cloudId, representationName).getBytes(), new MessageProperties());
        //when
        listener.onMessage(message);
        //then
        verifyZeroInteractions(solrDAO);
    }

    @Test
    public void shouldNotCallDAOWhenReceivedEmptyRepresentationName()
            throws Exception {
        //given
        String cloudId = "cloudId123";
        String representationName = "";
        Message message = new Message(prepareRemoveRepresentationMessage(cloudId, representationName).getBytes(), new MessageProperties());
        //when
        listener.onMessage(message);
        //then
        verifyZeroInteractions(solrDAO);
    }

    private String prepareRemoveRepresentationMessage(String cloudId, String representationName) {
        HashMap<String, String> map = new LinkedHashMap<>();
        map.put(ParamConstants.F_CLOUDID, cloudId);
        map.put(ParamConstants.F_REPRESENTATIONNAME, representationName);
        return gson.toJson(map);
    }

}
