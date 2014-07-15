package eu.europeana.cloud.service.dls.listeners;

import eu.europeana.cloud.service.dls.solr.SolrDAO;
import eu.europeana.cloud.service.mcs.messages.RemoveRecordRepresentationsMessage;
import java.io.IOException;
import org.apache.commons.lang.StringUtils;
import org.apache.solr.client.solrj.SolrServerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Listener that processes messages about removing all
 * {@link eu.europeana.cloud.common.model.Representation representations} from a
 * certain {@link eu.europeana.cloud.common.model.Record record}.
 * 
 * It receives messages with <code>record.representations.deleteAll</code>
 * routing key. Message text should be a cloudId of the record (directly sent
 * value).
 * 
 * After receiving properly formed message, listener calls
 * {@link eu.europeana.cloud.service.dls.solr.SolrDAO#removeRecordRepresentation(String)}
 * so that Solr index is updated (the
 * {@link eu.europeana.cloud.common.model.Record record} will have no
 * {@link eu.europeana.cloud.common.model.Representation representations} in
 * updated index).
 * 
 * If message is malformed, information about error is logged and no call to
 * {@link eu.europeana.cloud.service.dls.solr.SolrDAO} is performed. If call to
 * {@link eu.europeana.cloud.service.dls.solr.SolrDAO} fails, an information is
 * also logged.
 * 
 * Messages for this listener are produced by
 * <code>eu.europeana.cloud.service.mcs.persistent.SolrRepresentationIndexer.removeRecordRepresentations(String)}</code>
 * method in MCS.
 */
@Component
public class AllRecordRepresentationsRemovedListener implements
	MessageListener<RemoveRecordRepresentationsMessage> {

    private static final Logger LOGGER = LoggerFactory
	    .getLogger(RepresentationRemovedListener.class);

    @Autowired
    SolrDAO solrDAO;

    @Override
    public void onMessage(RemoveRecordRepresentationsMessage message) {
       String messageText = message.getPayload();
       
        if (messageText == null || messageText.isEmpty()) {
            LOGGER.error("Message has empty body.");
            return;
        }

        String cloudId = messageText;

        if (StringUtils.isBlank(cloudId)) {
            LOGGER.error("Required parameter cloud id is empty.");
            return;
        }

        try {
            solrDAO.removeRecordRepresentation(cloudId);
        } catch (SolrServerException | IOException ex) {
            LOGGER.error("Cannot remove representation from solr", ex);
        }

    }
}
