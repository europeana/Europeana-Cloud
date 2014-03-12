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
@ContextConfiguration(value = { "classpath:/testContext.xml" })
public class AllRecordRepresentationsRemovedistenerTest {

    @Autowired
    AllRecordRepresentationsRemovedistener listener;

    @Autowired
    SolrDAO solrDAO;


    @After
    public void cleanUp() {
        Mockito.reset(solrDAO);
    }


    @Test
    public void shouldCallRemoveRepresentation()
            throws Exception {

        String cloudId = "cloudId";
        Message message = new Message(cloudId.getBytes(), new MessageProperties());
        //when
        listener.onMessage(message);
        //then
        verify(solrDAO, times(1)).removeRecordRepresentation(cloudId);
        verifyNoMoreInteractions(solrDAO);
    }


    @Test
    public void shouldNotCallRemoveRecordRepresentationsWhenReceivedEmptyMessage()
            throws Exception {

        String cloudId = "";
        Message message = new Message(cloudId.getBytes(), new MessageProperties());
        //when
        listener.onMessage(message);
        //then
        verifyZeroInteractions(solrDAO);
    }


    @Test
    public void shouldNotCallRemoveRRecordRepresentationsWhenReceivedMessageWithNullBody()
            throws Exception {

        Message message = new Message(null, new MessageProperties());
        //when
        listener.onMessage(message);
        //then
        verifyZeroInteractions(solrDAO);
    }
}
