package eu.europeana.cloud.service.dls.listeners;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import eu.europeana.cloud.common.web.ParamConstants;
import eu.europeana.cloud.service.dls.solr.SolrDAO;
import eu.europeana.cloud.service.mcs.messages.RemoveRepresentationMessage;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(value = { "classpath:/testContext.xml" })
public class RepresentationRemovedMessageProcessorTest {

    @Autowired
    RepresentationRemovedMessageProcessor listener;

    @Autowired
    SolrDAO solrDAO;

    private static final Gson gson = new Gson();

    @Before
    public void cleanUp() {
	Mockito.reset(solrDAO);
    }

    @Test
    public void shouldCallRemoveRepresentation() throws Exception {
	// given
	String cloudId = "cloudId123";
	String representationName = "representation123";
	RemoveRepresentationMessage message = new RemoveRepresentationMessage(
		prepareRemoveRepresentationMessage(cloudId, representationName));
	// when
	listener.processMessage(message);
	// then
	verify(solrDAO, times(1)).removeRepresentation(cloudId,
		representationName);
	verifyNoMoreInteractions(solrDAO);
    }

    @Test
    public void shouldNotCallDAOWhenReceivedMessageWithNullBody()
	    throws Exception {
	// given
	RemoveRepresentationMessage message = new RemoveRepresentationMessage(
		null);
	// when
	listener.processMessage(message);
	// then
	verifyZeroInteractions(solrDAO);
    }

    @Test
    public void shouldNotCallDAOWhenReceivedEmptyMessage() throws Exception {
	// given
	RemoveRepresentationMessage message = new RemoveRepresentationMessage(
		"");
	// when
	listener.processMessage(message);
	// then
	verifyZeroInteractions(solrDAO);
    }

    @Test
    public void shouldNotCallDAOWhenReceivedNullCloudId() throws Exception {
	// given
	String cloudId = null;
	String representationName = "representation123";
	RemoveRepresentationMessage message = new RemoveRepresentationMessage(
		prepareRemoveRepresentationMessage(cloudId, representationName));
	// when
	listener.processMessage(message);
	// then
	verifyZeroInteractions(solrDAO);
    }

    @Test
    public void shouldNotCallDAOWhenReceivedEmptyCloudId() throws Exception {
	// given
	String cloudId = "";
	String representationName = "representation123";
	RemoveRepresentationMessage message = new RemoveRepresentationMessage(
		prepareRemoveRepresentationMessage(cloudId, representationName));
	// when
	listener.processMessage(message);
	// then
	verifyZeroInteractions(solrDAO);
    }

    @Test
    public void shouldNotCallDAOWhenReceivedNullRepresentationName()
	    throws Exception {
	// given
	String cloudId = "cloudId123";
	String representationName = null;
	RemoveRepresentationMessage message = new RemoveRepresentationMessage(
		prepareRemoveRepresentationMessage(cloudId, representationName));
	// when
	listener.processMessage(message);
	// then
	verifyZeroInteractions(solrDAO);
    }

    @Test
    public void shouldNotCallDAOWhenReceivedEmptyRepresentationName()
	    throws Exception {
	// given
	String cloudId = "cloudId123";
	String representationName = "";
	RemoveRepresentationMessage message = new RemoveRepresentationMessage(
		prepareRemoveRepresentationMessage(cloudId, representationName));
	// when
	listener.processMessage(message);
	// then
	verifyZeroInteractions(solrDAO);
    }

    private String prepareRemoveRepresentationMessage(String cloudId,
	    String representationName) {
	JsonObject jo = new JsonObject();
	jo.addProperty(ParamConstants.P_CLOUDID, cloudId);
	jo.addProperty(ParamConstants.P_REPRESENTATIONNAME, representationName);
	return jo.toString();
    }

}
