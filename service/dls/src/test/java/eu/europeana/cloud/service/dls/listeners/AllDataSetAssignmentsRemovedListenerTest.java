package eu.europeana.cloud.service.dls.listeners;

import com.google.gson.Gson;
import eu.europeana.cloud.common.model.CompoundDataSetId;
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
public class AllDataSetAssignmentsRemovedListenerTest {

    @Autowired
    AllDataSetAssignmentsRemovedListener listener;

    @Autowired
    SolrDAO solrDAO;

    private final Gson gson = new Gson();

    @After
    public void cleanUp() {
        Mockito.reset(solrDAO);
    }

    @Test
    public void shouldCallRemoveAssignmentFromDataSet()
            throws Exception {
        //given
        String dataSetId = "dataset123";
        String providerId = "provider123123";
        CompoundDataSetId compoundDataSetId = new CompoundDataSetId(providerId, dataSetId);
        Message message = new Message(prepareAllDataSetAssignmentsRemovedMessage(compoundDataSetId).getBytes(), new MessageProperties());
        //when
        listener.onMessage(message);
        //then
        verify(solrDAO, times(1)).removeAssignmentFromDataSet(compoundDataSetId);
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
    public void shouldNotCallDAOWhenReceivedNullCompundDataSetId()
            throws Exception {
        //given
        CompoundDataSetId compoundDataSetId = null;
        Message message = new Message(prepareAllDataSetAssignmentsRemovedMessage(compoundDataSetId).getBytes(), new MessageProperties());
        //when
        listener.onMessage(message);

        //then  
        verifyZeroInteractions(solrDAO);
    }

    @Test
    public void shouldNotCallDAOWhenReceivedNullProviderId()
            throws Exception {
        //given
        String dataSetId = "dataset123";
        String providerId = null;
        CompoundDataSetId compoundDataSetId = new CompoundDataSetId(providerId, dataSetId);
        Message message = new Message(prepareAllDataSetAssignmentsRemovedMessage(compoundDataSetId).getBytes(), new MessageProperties());
        //when
        listener.onMessage(message);

        //then  
        verifyZeroInteractions(solrDAO);
    }

    @Test
    public void shouldNotCallDAOWhenReceivedEmptyProviderId()
            throws Exception {
        //given
        String dataSetId = "dataset123";
        String providerId = "";
        CompoundDataSetId compoundDataSetId = new CompoundDataSetId(providerId, dataSetId);
        Message message = new Message(prepareAllDataSetAssignmentsRemovedMessage(compoundDataSetId).getBytes(), new MessageProperties());
        //when
        listener.onMessage(message);

        //then  
        verifyZeroInteractions(solrDAO);
    }

    @Test
    public void shouldNotCallDAOWhenReceivedNullDataSetId()
            throws Exception {
        //given
        String dataSetId = null;
        String providerId = "provider123123";
        CompoundDataSetId compoundDataSetId = new CompoundDataSetId(providerId, dataSetId);
        Message message = new Message(prepareAllDataSetAssignmentsRemovedMessage(compoundDataSetId).getBytes(), new MessageProperties());
        //when
        listener.onMessage(message);

        //then  
        verifyZeroInteractions(solrDAO);
    }

    @Test
    public void shouldNotCallDAOWhenReceivedEmptyDataSetId()
            throws Exception {
        //given
        String dataSetId = "";
        String providerId = "provider123123";
        CompoundDataSetId compoundDataSetId = new CompoundDataSetId(providerId, dataSetId);
        Message message = new Message(prepareAllDataSetAssignmentsRemovedMessage(compoundDataSetId).getBytes(), new MessageProperties());
        //when
        listener.onMessage(message);

        //then  
        verifyZeroInteractions(solrDAO);
    }

    private String prepareAllDataSetAssignmentsRemovedMessage(CompoundDataSetId compoundDataSetId) {
        return gson.toJson(compoundDataSetId);
    }
}
