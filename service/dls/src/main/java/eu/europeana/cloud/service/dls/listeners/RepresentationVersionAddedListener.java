package eu.europeana.cloud.service.dls.listeners;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.service.dls.solr.SolrDAO;
import eu.europeana.cloud.service.mcs.messages.InsertRepresentationMessage;
import java.io.IOException;
import org.apache.solr.client.solrj.SolrServerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Listener that processes messages about adding a new/updating
 * {@link eu.europeana.cloud.common.model.Representation representation} version
 * of a certain {@link eu.europeana.cloud.common.model.Record record}.
 * 
 * It receives messages with <code>records.representations.versions.add</code>
 * routing key. Message text should be a {@link Representation} object,
 * serialised to Json. {@link Representation} object contains all necessary
 * information (cloudId of the {@link eu.europeana.cloud.common.model.Record
 * record} it belongs to, version etc.).
 * 
 * After receiving properly formed message, listener calls
 * {@link eu.europeana.cloud.service.dls.solr.SolrDAO#insertRepresentation(Representation, Collection)
 * SolrDAO.insertRepresentation(Representation,
 * Collection&lt;CompoundDataSetId&gt;)} (with second argument being
 * <code>null</code>) so that Solr index is updated (
 * {@link eu.europeana.cloud.common.model.Record record} will hold a new
 * {@link eu.europeana.cloud.common.model.Representation representation} version
 * for a specific representation name (among others previously added) in updated
 * index).
 * 
 * If message is malformed, information about error is logged and no call to
 * {@link eu.europeana.cloud.service.dls.solr.SolrDAO} is performed. If call to
 * {@link eu.europeana.cloud.service.dls.solr.SolrDAO} fails, an information is
 * also logged.
 * 
 * Messages for this listener are produced by
 * <code>eu.europeana.cloud.service.mcs.persistent.SolrRepresentationIndexer.insertRepresentation(Representation)}</code>
 * method in MCS.
 */
@Component
public class RepresentationVersionAddedListener implements
	MessageListener<InsertRepresentationMessage> {

    private static final Logger LOGGER = LoggerFactory
	    .getLogger(RepresentationVersionAddedListener.class);
    private static final Gson gson = new GsonBuilder().setDateFormat(
	    "yyyy-MM-dd'T'HH:mm:ss.SSSZZ").create();

    @Autowired
    SolrDAO solrDao;

    @Override
    public void onMessage(InsertRepresentationMessage message) {
        String messageText = message.getPayload();
        
        if (messageText == null || messageText.isEmpty()) {
            LOGGER.error("Message has empty body.");
            return;
        }

        Representation representation = gson.fromJson(messageText, Representation.class);
        if (representation == null) {
            LOGGER.error("Received representation is null.");
            return;
        }

        try {
            solrDao.insertRepresentation(representation, null);
        } catch (IOException | SolrServerException ex) {
            LOGGER.error("Cannot insert representation into solr.", ex);
        }
    }
}
