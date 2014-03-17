package eu.europeana.cloud.service.dls.listeners;

import eu.europeana.cloud.service.dls.solr.SolrDAO;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(value = {"classpath:/testContext.xml"})
public class RepresentationVersionRemovedListenerTest {

    @Autowired
    RepresentationVersionRemovedListener listener;

    @Autowired
    SolrDAO solrDAO;

    @After
    public void cleanUp() {
        Mockito.reset(solrDAO);
    }

    @Test
    public void shouldCallRemoveRepresentation()
            throws Exception {
        //given
        String versionId = "version123";
        Message message = new Message(versionId.getBytes(), new MessageProperties());
        //when
        listener.onMessage(message);
        //then
        verify(solrDAO, times(1)).removeRepresentationVersion(versionId);
        verifyNoMoreInteractions(solrDAO);
    }

    @Test
    public void shouldNotCallRemoveRepresentationWhenReceivedNullMessage()
            throws Exception {
        //given
        Message message = new Message(null, new MessageProperties());
        //when
        listener.onMessage(message);
        //then
        verifyZeroInteractions(solrDAO);
    }

    @Test
    public void shouldNotCallRemoveRepresentationWhenReceivedEmptyMessage()
            throws Exception {
        //given
        String versionId = "";
        Message message = new Message(versionId.getBytes(), new MessageProperties());
        //when
        listener.onMessage(message);
        //then
        verifyZeroInteractions(solrDAO);
    }

}
