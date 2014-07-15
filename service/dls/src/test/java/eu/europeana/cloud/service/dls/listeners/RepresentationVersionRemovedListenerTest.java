package eu.europeana.cloud.service.dls.listeners;

import eu.europeana.cloud.service.dls.solr.SolrDAO;
import eu.europeana.cloud.service.mcs.messages.RemoveRepresentationVersionMessage;
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
public class RepresentationVersionRemovedListenerTest {

    @Autowired
    RepresentationVersionRemovedListener listener;

    @Autowired
    SolrDAO solrDAO;

    @Before
    public void cleanUp() {
	Mockito.reset(solrDAO);
    }

    @Test
    public void shouldCallRemoveRepresentation() throws Exception {
	// given
	String versionId = "version123";
	RemoveRepresentationVersionMessage message = new RemoveRepresentationVersionMessage(
		versionId);
	// when
	listener.onMessage(message);
	// then
	verify(solrDAO, times(1)).removeRepresentationVersion(versionId);
	verifyNoMoreInteractions(solrDAO);
    }

    @Test
    public void shouldNotCallRemoveRepresentationWhenReceivedNullMessage()
	    throws Exception {
	// given
	RemoveRepresentationVersionMessage message = new RemoveRepresentationVersionMessage(
		null);
	// when
	listener.onMessage(message);
	// then
	verifyZeroInteractions(solrDAO);
    }

    @Test
    public void shouldNotCallRemoveRepresentationWhenReceivedEmptyMessage()
	    throws Exception {
	// given
	String versionId = "";
	RemoveRepresentationVersionMessage message = new RemoveRepresentationVersionMessage(
		versionId);
	// when
	listener.onMessage(message);
	// then
	verifyZeroInteractions(solrDAO);
    }

}
