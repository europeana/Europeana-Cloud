package eu.europeana.cloud.service.dls.listeners;

import eu.europeana.cloud.service.dls.solr.SolrDAO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageListener;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Listener class which reacts on messages about
 */
public class RepresentationVersionAddedListener implements MessageListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(RepresentationVersionRemovedListener.class);

    @Autowired
    SolrDAO solrDao;


    /**
     * 
     * @param message
     */
    @Override
    public void onMessage(Message message) {

    }
}
