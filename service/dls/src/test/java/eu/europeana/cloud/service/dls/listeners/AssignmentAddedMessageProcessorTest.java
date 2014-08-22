package eu.europeana.cloud.service.dls.listeners;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import eu.europeana.cloud.common.model.CompoundDataSetId;
import eu.europeana.cloud.common.web.ParamConstants;
import eu.europeana.cloud.service.dls.solr.SolrDAO;
import eu.europeana.cloud.service.mcs.messages.AddAssignmentMessage;
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
public class AssignmentAddedMessageProcessorTest {

    @Autowired
    AssignmentAddedMessageProcessor listener;

    @Autowired
    SolrDAO solrDAO;

    private static final Gson gson = new Gson();

    @Before
    public void cleanUp() {
	Mockito.reset(solrDAO);
    }

    @Test
    public void shouldCallAddAssignment() throws Exception {
	String version = "version123123";
	String dataSet = "dataSet12";
	String provider = "provider123";
	CompoundDataSetId ds = new CompoundDataSetId(provider, dataSet);
	AddAssignmentMessage message = new AddAssignmentMessage(
		prepareAddAssignmentMessage(version, ds));
	listener.processMessage(message);
	verify(solrDAO, times(1)).addAssignment(version, ds);
	verifyNoMoreInteractions(solrDAO);

    }

    @Test
    public void shouldNotCallDAOWhenReceivedMessageWithNullBody()
	    throws Exception {
	// given
	AddAssignmentMessage message = new AddAssignmentMessage(null);
	// when
	listener.processMessage(message);
	// then
	verifyZeroInteractions(solrDAO);
    }

    @Test
    public void shouldNotCallDAOWhenReceivedEmptyMessage() throws Exception {
	// given
	AddAssignmentMessage message = new AddAssignmentMessage("");
	// when
	listener.processMessage(message);
	// then
	verifyZeroInteractions(solrDAO);
    }

    @Test
    public void shouldNotCallDAOWhenReceivedNullVersion() throws Exception {
	// given
	String version = null;
	String dataSet = "dataSet";
	String provider = "provider123";
	CompoundDataSetId ds = new CompoundDataSetId(provider, dataSet);
	AddAssignmentMessage message = new AddAssignmentMessage(
		prepareAddAssignmentMessage(version, ds));
	// when
	listener.processMessage(message);
	// then
	verifyZeroInteractions(solrDAO);
    }

    @Test
    public void shouldNotCallDAOWhenReceivedEmptyVersion() throws Exception {
	// given
	String version = "";
	String dataSet = "dataSet";
	String provider = "provider123";
	CompoundDataSetId ds = new CompoundDataSetId(provider, dataSet);
	AddAssignmentMessage message = new AddAssignmentMessage(
		prepareAddAssignmentMessage(version, ds));
	// when
	listener.processMessage(message);
	// then
	verifyZeroInteractions(solrDAO);
    }

    @Test
    public void shouldNotCallDAOWhenReceivedNullCompundDataSetId()
	    throws Exception {
	// given
	String version = "version123123";
	CompoundDataSetId ds = null;
	AddAssignmentMessage message = new AddAssignmentMessage(
		prepareAddAssignmentMessage(version, ds));
	// when
	listener.processMessage(message);
	// then
	verifyZeroInteractions(solrDAO);
    }

    @Test
    public void shouldNotCallDAOWhenReceivedNullDataSet() throws Exception {
	// given
	String version = "version123123";
	String dataSet = null;
	String provider = "provider123";
	CompoundDataSetId ds = new CompoundDataSetId(provider, dataSet);
	AddAssignmentMessage message = new AddAssignmentMessage(
		prepareAddAssignmentMessage(version, ds));
	// when
	listener.processMessage(message);
	// then
	verifyZeroInteractions(solrDAO);
    }

    @Test
    public void shouldNotCallDAOWhenReceivedEmptyDataSet() throws Exception {
	// given
	String version = "version123123";
	String dataSet = "";
	String provider = "provider123";
	CompoundDataSetId ds = new CompoundDataSetId(provider, dataSet);
	AddAssignmentMessage message = new AddAssignmentMessage(
		prepareAddAssignmentMessage(version, ds));
	// when
	listener.processMessage(message);
	// then
	verifyZeroInteractions(solrDAO);
    }

    @Test
    public void shouldNotCallDAOWhenReceivedNullProvider() throws Exception {
	// given
	String version = "version123123";
	String dataSet = "dataSet12";
	String provider = null;
	CompoundDataSetId ds = new CompoundDataSetId(provider, dataSet);
	AddAssignmentMessage message = new AddAssignmentMessage(
		prepareAddAssignmentMessage(version, ds));
	// when
	listener.processMessage(message);
	// then
	verifyZeroInteractions(solrDAO);
    }

    @Test
    public void shouldNotCallDAOWhenReceivedEmptyProvider() throws Exception {
	// given
	String version = "version123123";
	String dataSet = "dataSet12";
	String provider = "";
	CompoundDataSetId ds = new CompoundDataSetId(provider, dataSet);
	AddAssignmentMessage message = new AddAssignmentMessage(
		prepareAddAssignmentMessage(version, ds));
	// when
	listener.processMessage(message);
	// then
	verifyZeroInteractions(solrDAO);
    }

    private String prepareAddAssignmentMessage(String versionId,
	    CompoundDataSetId dataSetId) {
	JsonElement elem = gson.toJsonTree(dataSetId, CompoundDataSetId.class);
	JsonObject jo = new JsonObject();
	jo.add(ParamConstants.F_DATASET_PROVIDER_ID, elem);
	jo.addProperty(ParamConstants.P_VER, versionId);
	return jo.toString();
    }
}
