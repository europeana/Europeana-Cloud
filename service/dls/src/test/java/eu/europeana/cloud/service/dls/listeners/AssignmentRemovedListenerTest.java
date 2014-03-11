package eu.europeana.cloud.service.dls.listeners;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import eu.europeana.cloud.common.web.ParamConstants;
import eu.europeana.cloud.service.dls.solr.CompoundDataSetId;
import eu.europeana.cloud.service.dls.solr.SolrDAO;
import java.util.Collections;
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
public class AssignmentRemovedListenerTest {

    @Autowired
    AssignmentRemovedListener listener;

    @Autowired
    SolrDAO solrDAO;

    private static final Gson gson = new Gson();


    @After
    public void cleanUp() {
        Mockito.reset(solrDAO);
    }


    private String prepareRemoveAssginmentMessage(String cloudId, String representationName, CompoundDataSetId dataSetId) {
        JsonElement elem = gson.toJsonTree(dataSetId, CompoundDataSetId.class);
        JsonObject jo = new JsonObject();
        jo.add("compoundDataSetId", elem);
        jo.addProperty(ParamConstants.P_CLOUDID, cloudId);
        jo.addProperty(ParamConstants.P_REPRESENTATIONNAME, representationName);
        return jo.toString();
    }


    @Test
    public void shouldCallRremoveAssignment()
            throws Exception {
        String cloudId = "id123123";
        String representationName = "rn";
        String dataSet = "dataset";
        String provider = "provider123";
        CompoundDataSetId ds = new CompoundDataSetId(provider, dataSet);
        Message message = new Message(prepareRemoveAssginmentMessage(cloudId, representationName, ds).getBytes(),
                new MessageProperties());
        listener.onMessage(message);
        verify(solrDAO, times(1)).removeAssignment(cloudId, representationName, Collections.singletonList(ds));
        verifyNoMoreInteractions(solrDAO);

    }


    @Test
    public void shouldNotCallRemoveAssignmentWhenReceivedEmptyMessage()
            throws Exception {

        Message message = new Message(new String("").getBytes(), new MessageProperties());
        //when
        listener.onMessage(message);
        //then
        verifyZeroInteractions(solrDAO);
    }


    @Test
    public void shouldNotCallDAOWhenReceivedMessageWithEmptyCloudIdProperty()
            throws Exception {

        String cloudId = "";
        String representationName = "rn";
        String dataSet = "dataset";
        String provider = "provider123";
        CompoundDataSetId ds = new CompoundDataSetId(provider, dataSet);
        Message message = new Message(prepareRemoveAssginmentMessage(cloudId, representationName, ds).getBytes(),
                new MessageProperties());
        //when
        listener.onMessage(message);
        //then
        verifyZeroInteractions(solrDAO);
    }


    @Test
    public void shouldNotCallDAOWhenReceivedMessageWithEmptyRepresentationNameProperty()
            throws Exception {

        String cloudId = "id";
        String representationName = "";
        String dataSet = "dataset";
        String provider = "provider123";
        CompoundDataSetId ds = new CompoundDataSetId(provider, dataSet);
        Message message = new Message(prepareRemoveAssginmentMessage(cloudId, representationName, ds).getBytes(),
                new MessageProperties());
        //when
        listener.onMessage(message);
        //then
        verifyZeroInteractions(solrDAO);
    }


    @Test
    public void shouldNotCallDAOWhenReceivedMessageWithEmptyDataSetProperty()
            throws Exception {

        String cloudId = "cloud123123";
        String representationName = "rn";
        String dataSet = "";
        String provider = "provider123";
        CompoundDataSetId ds = new CompoundDataSetId(provider, dataSet);
        Message message = new Message(prepareRemoveAssginmentMessage(cloudId, representationName, ds).getBytes(),
                new MessageProperties());
        //when
        listener.onMessage(message);
        //then
        verifyZeroInteractions(solrDAO);
    }


    @Test
    public void shouldNotCallDAOWhenReceivedMessageWithEmptyProviderProperty()
            throws Exception {

        String cloudId = "cloud123123";
        String representationName = "rn";
        String dataSet = "dataSet";
        String provider = "";
        CompoundDataSetId ds = new CompoundDataSetId(provider, dataSet);
        Message message = new Message(prepareRemoveAssginmentMessage(cloudId, representationName, ds).getBytes(),
                new MessageProperties());
        //when
        listener.onMessage(message);
        //then
        verifyZeroInteractions(solrDAO);
    }


    @Test
    public void shouldNotCallDAOWhenReceivedMessageWithNullBody()
            throws Exception {

        Message message = new Message(null, new MessageProperties());
        //when
        listener.onMessage(message);
        //then
        verifyZeroInteractions(solrDAO);
    }

}
