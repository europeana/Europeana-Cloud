package eu.europeana.cloud.service.dls.listeners;

import eu.europeana.cloud.service.dls.solr.SolrDAO;
import java.io.IOException;
import org.apache.solr.client.solrj.SolrServerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageListener;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * 
 * 
 */
public class AllRecordRepresentationsRemovedListener implements MessageListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(RepresentationRemovedListener.class);

    @Autowired
    SolrDAO solrDAO;


    @Override
    public void onMessage(Message message) {
        if (message.getBody() != null) {
            String cloudId = new String(message.getBody());
            if (!cloudId.isEmpty()) {
                try {
                    solrDAO.removeRecordRepresentation(cloudId);
                } catch (SolrServerException | IOException ex) {
                    LOGGER.error("Cannot remove representation from solr", ex);
                }
            } else {
                LOGGER.error("Message has empty body");
            }
        } else {
            LOGGER.error("Message has null body");
        }
    }

}
