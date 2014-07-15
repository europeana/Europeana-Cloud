package eu.europeana.cloud.service.dls.listeners;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import eu.europeana.cloud.common.model.File;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.service.dls.solr.SolrDAO;
import eu.europeana.cloud.service.mcs.messages.InsertRepresentationMessage;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(value = { "classpath:/testContext.xml" })
public class RepresentationVersionAddedListenerTest {

    @Autowired
    RepresentationVersionAddedListener listener;

    @Autowired
    SolrDAO solrDAO;

    private static final Gson gson = new GsonBuilder().setDateFormat(
	    "yyyy-MM-dd'T'HH:mm:ss.SSSZZ").create();

    @Before
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
        InsertRepresentationMessage message = new InsertRepresentationMessage(prepareInsertRepresentationMessage(representation));
        //when
        listener.onMessage(message);

        //then
        verify(solrDAO, times(1)).insertRepresentation(representation, null);
        verifyNoMoreInteractions(solrDAO);

    }

    @Test
    public void shouldNotCallDAOWhenReceivedMessageWithNullBody()
	    throws Exception {
	// given
	InsertRepresentationMessage message = new InsertRepresentationMessage(
		null);
	// when
	listener.onMessage(message);
	// then
	verifyZeroInteractions(solrDAO);
    }

    @Test
    public void shouldNotCallDAOWhenReceivedEmptyMessage() throws Exception {
	// given
	InsertRepresentationMessage message = new InsertRepresentationMessage(
		"");
	// when
	listener.onMessage(message);
	// then
	verifyZeroInteractions(solrDAO);
    }

    @Test
    public void shouldNotCallDAOWhenReceivedNullRepresentation()
	    throws Exception {
	// given
	InsertRepresentationMessage message = new InsertRepresentationMessage(
		prepareInsertRepresentationMessage(null));
	// when
	listener.onMessage(message);
	// then
	verifyZeroInteractions(solrDAO);
    }

    private String prepareInsertRepresentationMessage(
	    Representation representation) {
	return gson.toJson(representation);
    }
}
