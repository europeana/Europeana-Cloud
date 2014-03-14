package eu.europeana.cloud.service.dls.listeners;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import eu.europeana.cloud.common.model.File;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.web.ParamConstants;
import eu.europeana.cloud.service.dls.solr.CompoundDataSetId;
import eu.europeana.cloud.service.dls.solr.SolrDAO;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
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
public class RepresentationVersionAddedListenerTest {

    @Autowired
    RepresentationVersionAddedListener listener;

    @Autowired
    SolrDAO solrDAO;

    private static final Gson gson = new GsonBuilder()
            .setDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZZ")
            .create();

    @After
    public void cleanUp() {
        Mockito.reset(solrDAO);
    }

    @Test
    public void shouldCallInsertRepresentation()
            throws Exception {
        //given
        String providerId = "Provider001";
        String cloudId = "25DG622J4VM";
        String representationName = "representation01";
        String versionId = "b95fcda0-994a-11e3-bfe1-1c6f653f6012";
        ArrayList<File> files = new ArrayList<>();
        boolean persistent = false;
        Date creationDate = Calendar.getInstance().getTime();

        Representation representation = new Representation(cloudId, representationName, versionId, null, null, providerId, files, persistent, creationDate);
        Message message = new Message(prepareInsertRepresentationMessage(representation).getBytes(), new MessageProperties());

        //when
        listener.onMessage(message);

        //then
        verify(solrDAO, times(1)).insertRepresentation(representation, null);
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
    public void shouldNotCallDAOWhenReceivedNullRepresentation()
            throws Exception {
        //given
        Message message = new Message(prepareInsertRepresentationMessage(null).getBytes(), new MessageProperties());
        //when
        listener.onMessage(message);
        //then
        verifyZeroInteractions(solrDAO);
    }

    private String prepareInsertRepresentationMessage(Representation representation) {
        return gson.toJson(representation);
    }
}
