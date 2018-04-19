package eu.europeana.cloud.service.dls.listeners;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import eu.europeana.cloud.common.model.CompoundDataSetId;
import eu.europeana.cloud.common.model.File;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.model.Revision;
import eu.europeana.cloud.common.web.ParamConstants;
import eu.europeana.cloud.service.dls.solr.SolrDAO;
import eu.europeana.cloud.service.mcs.messages.InsertRepresentationPersistentMessage;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.*;

import static org.mockito.Mockito.*;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(value = { "classpath:/testContext.xml" })
public class RepresentationVersionAddedPersistentMessageProcessorTest {

    @Autowired
    RepresentationVersionAddedPersistentMessageProcessor listener;

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
        ArrayList<File> files = new ArrayList<>(0);
        ArrayList<Revision> revisions = new ArrayList<>(0);
        boolean persistent = false;
        Date creationDate = Calendar.getInstance().getTime();
        Representation representation = new Representation(cloudId, representationName, versionId, null, null, providerId, files,revisions, persistent, creationDate);

        String dataSetId1 = "dataSet1";
        String dataSetId2 = "dataSet2";
        Collection<CompoundDataSetId> dataSetIds = new ArrayList<>(2);
        dataSetIds.add(new CompoundDataSetId(providerId, dataSetId1));
        dataSetIds.add(new CompoundDataSetId(providerId, dataSetId2));

        InsertRepresentationPersistentMessage message = new InsertRepresentationPersistentMessage(prepareInsertPersistentRepresentationMessage(representation, dataSetIds));
        //when
        listener.processMessage(message);

        //then
        verify(solrDAO, times(1)).insertRepresentation(representation, dataSetIds);
        verifyNoMoreInteractions(solrDAO);

    }

    @Test
    public void shouldNotCallDAOWhenReceivedMessageWithNullBody()
	    throws Exception {
	// given
	InsertRepresentationPersistentMessage message = new InsertRepresentationPersistentMessage(
		null);
	// when
	listener.processMessage(message);
	// then
	verifyZeroInteractions(solrDAO);
    }

    @Test
    public void shouldNotCallDAOWhenReceivedEmptyMessage() throws Exception {
	// given
	InsertRepresentationPersistentMessage message = new InsertRepresentationPersistentMessage(
		"");
	// when
	listener.processMessage(message);
	// then
	verifyZeroInteractions(solrDAO);
    }

    @Test
    public void shouldNotCallDAOWhenReceivedNullMap() throws Exception {
	// given
	HashMap<String, Object> map = null;
	InsertRepresentationPersistentMessage message = new InsertRepresentationPersistentMessage(
		gson.toJson(map));
	// when
	listener.processMessage(message);
	// then
	verifyZeroInteractions(solrDAO);
    }

    @Test
    public void shouldNotCallDAOWhenReceivedEmptyMap()
            throws Exception {
        //given
        HashMap<String, Object> map = new LinkedHashMap<>();
        InsertRepresentationPersistentMessage message = new InsertRepresentationPersistentMessage(gson.toJson(map));
        //when
        listener.processMessage(message);
        //then
        verifyZeroInteractions(solrDAO);
    }

    @Test
    public void shouldNotCallDAOWhenReceivedNullRepresentation()
            throws Exception {
        //given
        String providerId = "Provider001";
        String dataSetId1 = "dataSet1";
        String dataSetId2 = "dataSet2";
        Collection<CompoundDataSetId> dataSetIds = new ArrayList<>(2);
        dataSetIds.add(new CompoundDataSetId(providerId, dataSetId1));
        dataSetIds.add(new CompoundDataSetId(providerId, dataSetId2));

        InsertRepresentationPersistentMessage message = new InsertRepresentationPersistentMessage(prepareInsertPersistentRepresentationMessage(null, dataSetIds));
        //when
        listener.processMessage(message);
        //then
        verifyZeroInteractions(solrDAO);
    }

    @Test
    public void shouldNotCallDAOWhenReceivedNullDataSetIds()
            throws Exception {
        //given
        String providerId = "Provider001";
        String cloudId = "25DG622J4VM";
        String representationName = "representation01";
        String versionId = "b95fcda0-994a-11e3-bfe1-1c6f653f6012";
        ArrayList<File> files = new ArrayList<>(0);
        ArrayList<Revision> revisions = new ArrayList<>(0);

        boolean persistent = false;
        Date creationDate = Calendar.getInstance().getTime();

        Representation representation = new Representation(cloudId, representationName, versionId, null, null, providerId, files,revisions, persistent, creationDate);
        InsertRepresentationPersistentMessage message = new InsertRepresentationPersistentMessage(prepareInsertPersistentRepresentationMessage(representation, null));

        //when
        listener.processMessage(message);
        //then
        verifyZeroInteractions(solrDAO);
    }

    private String prepareInsertPersistentRepresentationMessage(Representation representation, Collection<CompoundDataSetId> dataSetIds) {
        HashMap<String, Object> map = new LinkedHashMap<>();
        map.put(ParamConstants.F_REPRESENTATION, representation);
        map.put(ParamConstants.F_DATASETS, dataSetIds);
        return gson.toJson(map);
    }
}
