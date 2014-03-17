package eu.europeana.cloud.service.dls.listeners;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
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
@ContextConfiguration(value = { "classpath:/testContext.xml" })
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
    public void shouldCallRemoveAssignments()
            throws Exception {
        //given
        CompoundDataSetId dataSetId = new CompoundDataSetId("providerId", "dataSetId");
        JsonElement elem = gson.toJsonTree(dataSetId, CompoundDataSetId.class);
        JsonObject jo = new JsonObject();
        jo.add("compoundDataSetId", elem);

        Message message = new Message(jo.toString().getBytes(), new MessageProperties());
        //when
        listener.onMessage(message);
        //then
        verify(solrDAO, times(1)).removeAssignmentFromDataSet(dataSetId);
        verifyNoMoreInteractions(solrDAO);
    }


    @Test
    public void shouldNotCallRemoveAssignmentsWhenReceivedEmptyMessage()
            throws Exception {

        //given
        String versionId = "";
        Message message = new Message(versionId.getBytes(), new MessageProperties());
        //when
        listener.onMessage(message);
        //then
        verifyZeroInteractions(solrDAO);
    }


    @Test
    public void shouldNotCallRemoveAssignmentsWhenReceivedMessageWithNullBody()
            throws Exception {

        //given
        Message message = new Message(null, new MessageProperties());
        //when
        listener.onMessage(message);
        //then
        verifyZeroInteractions(solrDAO);
    }


    @Test
    public void shouldNotCallRemoveAssignmentsWhenMessageContainsNullProviderParameter()
            throws Exception {
        //given
        CompoundDataSetId dataSetId = new CompoundDataSetId(null, "dataSetId");
        JsonElement elem = gson.toJsonTree(dataSetId, CompoundDataSetId.class);
        JsonObject jo = new JsonObject();
        jo.add("compoundDataSetId", elem);

        Message message = new Message(jo.toString().getBytes(), new MessageProperties());
        //when
        listener.onMessage(message);

        //then  
        verifyZeroInteractions(solrDAO);
    }


    @Test
    public void shouldNotCallRemoveAssignmentsWhenMessageContainsEmptyProviderParameter()
            throws Exception {
        //given
        CompoundDataSetId dataSetId = new CompoundDataSetId("", "dataSetId");
        JsonElement elem = gson.toJsonTree(dataSetId, CompoundDataSetId.class);
        JsonObject jo = new JsonObject();
        jo.add("compoundDataSetId", elem);

        Message message = new Message(jo.toString().getBytes(), new MessageProperties());
        //when
        listener.onMessage(message);

        //then  
        verifyZeroInteractions(solrDAO);
    }


    @Test
    public void shouldNotCallRemoveAssignmentsWhenMessageContainsNullDataSetIdParameter()
            throws Exception {
        //given
        CompoundDataSetId dataSetId = new CompoundDataSetId("dataSetId", null);
        JsonElement elem = gson.toJsonTree(dataSetId, CompoundDataSetId.class);
        JsonObject jo = new JsonObject();
        jo.add("compoundDataSetId", elem);

        Message message = new Message(jo.toString().getBytes(), new MessageProperties());
        //when
        listener.onMessage(message);

        //then  
        verifyZeroInteractions(solrDAO);
    }


    @Test
    public void shouldNotCallRemoveAssignmentsWhenMessageContainsEmptyDataSetIdParameter()
            throws Exception {
        //given
        CompoundDataSetId dataSetId = new CompoundDataSetId("providerId", "");
        JsonElement elem = gson.toJsonTree(dataSetId, CompoundDataSetId.class);
        JsonObject jo = new JsonObject();
        jo.add("compoundDataSetId", elem);

        Message message = new Message(jo.toString().getBytes(), new MessageProperties());
        //when
        listener.onMessage(message);

        //then  
        verifyZeroInteractions(solrDAO);
    }
}
