package eu.europeana.cloud.service.dls.listeners;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import eu.europeana.cloud.common.model.CompoundDataSetId;
import eu.europeana.cloud.common.web.ParamConstants;
import eu.europeana.cloud.service.dls.solr.SolrDAO;
import eu.europeana.cloud.service.mcs.messages.RemoveAssignmentMessage;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.Collections;

import static org.mockito.Mockito.*;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(value = { "classpath:/testContext.xml" })
public class AssignmentRemovedMessageProcessorTest {

    @Autowired
    AssignmentRemovedMessageProcessor listener;

    @Autowired
    SolrDAO solrDAO;

    private static final Gson gson = new Gson();

    @Before
    public void cleanUp() {
	Mockito.reset(solrDAO);
    }

    @Test
    public void shouldCallRremoveAssignment() throws Exception {
	// given
	String cloudId = "id123123";
	String representationName = "rn";
	String dataSet = "dataset";
	String provider = "provider123";
	CompoundDataSetId ds = new CompoundDataSetId(provider, dataSet);
	RemoveAssignmentMessage message = new RemoveAssignmentMessage(
		prepareRemoveAssginmentMessage(cloudId, representationName, ds));
	// when
	listener.processMessage(message);
	// then
	verify(solrDAO, times(1)).removeAssignment(cloudId, representationName,
		Collections.singletonList(ds));
	verifyNoMoreInteractions(solrDAO);

    }

    @Test
    public void shouldNotCallDAOWhenReceivedMessageWithNullBody()
	    throws Exception {
	// given
	RemoveAssignmentMessage message = new RemoveAssignmentMessage(null);
	// when
	listener.processMessage(message);
	// then
	verifyZeroInteractions(solrDAO);
    }

    @Test
    public void shouldNotCallDAOWhenReceivedEmptyMessage() throws Exception {
	// given
	RemoveAssignmentMessage message = new RemoveAssignmentMessage("");
	// when
	listener.processMessage(message);
	// then
	verifyZeroInteractions(solrDAO);
    }

    @Test
    public void shouldNotCallDAOWhenReceivedNullCloudId() throws Exception {
	// given
	String cloudId = null;
	String representationName = "rn";
	String dataSet = "dataset";
	String provider = "provider123";
	CompoundDataSetId ds = new CompoundDataSetId(provider, dataSet);
	RemoveAssignmentMessage message = new RemoveAssignmentMessage(
		prepareRemoveAssginmentMessage(cloudId, representationName, ds));
	// when
	listener.processMessage(message);
	// then
	verifyZeroInteractions(solrDAO);
    }

    @Test
    public void shouldNotCallDAOWhenReceivedEmptyCloudId() throws Exception {
	// given
	String cloudId = "";
	String representationName = "rn";
	String dataSet = "dataset";
	String provider = "provider123";
	CompoundDataSetId ds = new CompoundDataSetId(provider, dataSet);
	RemoveAssignmentMessage message = new RemoveAssignmentMessage(
		prepareRemoveAssginmentMessage(cloudId, representationName, ds));
	// when
	listener.processMessage(message);
	// then
	verifyZeroInteractions(solrDAO);
    }

    @Test
    public void shouldNotCallDAOWhenReceivedNullRepresentationName()
	    throws Exception {
	// given
	String cloudId = "id123123";
	String representationName = null;
	String dataSet = "dataset";
	String provider = "provider123";
	CompoundDataSetId ds = new CompoundDataSetId(provider, dataSet);
	RemoveAssignmentMessage message = new RemoveAssignmentMessage(
		prepareRemoveAssginmentMessage(cloudId, representationName, ds));
	// when
	listener.processMessage(message);
	// then
	verifyZeroInteractions(solrDAO);
    }

    @Test
    public void shouldNotCallDAOWhenReceivedEmptyRepresentationName()
	    throws Exception {
	// given
	String cloudId = "id123123";
	String representationName = "";
	String dataSet = "dataset";
	String provider = "provider123";
	CompoundDataSetId ds = new CompoundDataSetId(provider, dataSet);
	RemoveAssignmentMessage message = new RemoveAssignmentMessage(
		prepareRemoveAssginmentMessage(cloudId, representationName, ds));
	// when
	listener.processMessage(message);
	// then
	verifyZeroInteractions(solrDAO);
    }

    @Test
    public void shouldNotCallDAOWhenReceivedNullCompundDataSetId()
	    throws Exception {
	// given
	String cloudId = "id123123";
	String representationName = "rn";
	RemoveAssignmentMessage message = new RemoveAssignmentMessage(
		prepareRemoveAssginmentMessage(cloudId, representationName,
			null));
	// when
	listener.processMessage(message);
	// then
	verifyZeroInteractions(solrDAO);
    }

    @Test
    public void shouldNotCallDAOWhenReceivedNullDataSet() throws Exception {
	// given
	// given
	String cloudId = "id123123";
	String representationName = "rn";
	String dataSet = null;
	String provider = "provider123";
	CompoundDataSetId ds = new CompoundDataSetId(provider, dataSet);
	RemoveAssignmentMessage message = new RemoveAssignmentMessage(
		prepareRemoveAssginmentMessage(cloudId, representationName, ds));
	// when
	listener.processMessage(message);
	// then
	verifyZeroInteractions(solrDAO);
    }

    @Test
    public void shouldNotCallDAOWhenReceivedEmptyDataSet() throws Exception {
	// given
	String cloudId = "id123123";
	String representationName = "rn";
	String dataSet = "";
	String provider = "provider123";
	CompoundDataSetId ds = new CompoundDataSetId(provider, dataSet);
	RemoveAssignmentMessage message = new RemoveAssignmentMessage(
		prepareRemoveAssginmentMessage(cloudId, representationName, ds));
	// when
	listener.processMessage(message);
	// then
	verifyZeroInteractions(solrDAO);
    }

    @Test
    public void shouldNotCallDAOWhenReceivedNullProvider() throws Exception {
	// given
	String cloudId = "id123123";
	String representationName = "rn";
	String dataSet = "dataset";
	String provider = null;
	CompoundDataSetId ds = new CompoundDataSetId(provider, dataSet);
	RemoveAssignmentMessage message = new RemoveAssignmentMessage(
		prepareRemoveAssginmentMessage(cloudId, representationName, ds));
	// when
	listener.processMessage(message);
	// then
	verifyZeroInteractions(solrDAO);
    }

    @Test
    public void shouldNotCallDAOWhenReceivedEmptyProvider() throws Exception {
	// given
	String cloudId = "id123123";
	String representationName = "rn";
	String dataSet = "dataset";
	String provider = "";
	CompoundDataSetId ds = new CompoundDataSetId(provider, dataSet);
	RemoveAssignmentMessage message = new RemoveAssignmentMessage(
		prepareRemoveAssginmentMessage(cloudId, representationName, ds));
	// when
	listener.processMessage(message);
	// then
	verifyZeroInteractions(solrDAO);
    }

    private String prepareRemoveAssginmentMessage(String cloudId,
	    String representationName, CompoundDataSetId dataSetId) {
	JsonElement elem = gson.toJsonTree(dataSetId, CompoundDataSetId.class);
	JsonObject jo = new JsonObject();
	jo.add(ParamConstants.F_DATASET_PROVIDER_ID, elem);
	jo.addProperty(ParamConstants.P_CLOUDID, cloudId);
	jo.addProperty(ParamConstants.P_REPRESENTATIONNAME, representationName);
	return jo.toString();
    }
}
