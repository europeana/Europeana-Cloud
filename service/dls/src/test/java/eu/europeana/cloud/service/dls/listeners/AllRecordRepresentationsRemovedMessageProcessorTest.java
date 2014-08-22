package eu.europeana.cloud.service.dls.listeners;

import eu.europeana.cloud.service.dls.solr.SolrDAO;
import eu.europeana.cloud.service.mcs.messages.RemoveRecordRepresentationsMessage;
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
public class AllRecordRepresentationsRemovedMessageProcessorTest {

    @Autowired
    AllRecordRepresentationsRemovedMessageProcessor listener;

    @Autowired
    SolrDAO solrDAO;

    @Before
    public void cleanUp() {
	Mockito.reset(solrDAO);
    }

    @Test
    public void shouldCallRemoveRecordsRepresentation() throws Exception {

	String cloudId = "cloudId123";
	RemoveRecordRepresentationsMessage message = new RemoveRecordRepresentationsMessage(
		cloudId);
	// when
	listener.processMessage(message);
	// then
	verify(solrDAO, times(1)).removeRecordRepresentation(cloudId);
	verifyNoMoreInteractions(solrDAO);
    }

    @Test
    public void shouldNotCallRemoveRepresentationWhenReceivedNullMessage()
	    throws Exception {
	// given
	RemoveRecordRepresentationsMessage message = new RemoveRecordRepresentationsMessage(
		null);
	// when
	listener.processMessage(message);
	// then
	verifyZeroInteractions(solrDAO);
    }

    @Test
    public void shouldNotCallRemoveRepresentationWhenReceivedEmptyMessage()
	    throws Exception {

	String cloudId = "";
	RemoveRecordRepresentationsMessage message = new RemoveRecordRepresentationsMessage(
		cloudId);
	// when
	listener.processMessage(message);
	// then
	verifyZeroInteractions(solrDAO);
    }

}
