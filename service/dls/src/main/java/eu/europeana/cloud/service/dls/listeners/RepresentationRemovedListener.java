package eu.europeana.cloud.service.dls.listeners;

import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import eu.europeana.cloud.common.web.ParamConstants;
import eu.europeana.cloud.service.dls.solr.SolrDAO;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.LinkedHashMap;
import org.apache.solr.client.solrj.SolrServerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Listener class which reacts on messages about deletion of record's representation.
 */
@Component
public class RepresentationRemovedListener implements MessageListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(RepresentationRemovedListener.class);

    @Autowired
    SolrDAO solrDao;

    private final Gson gson = new Gson();


    /**
     * Reacts on message about represention removal and removes all versions of requested representation from index.
     * Message body should contain cloud identifier and schema of the representation.
     * 
     * @param message
     */
    @Override
    public void onMessage(Message message) {
        String body = new String(message.getBody());
        if (!body.isEmpty()) {
            Type type = new TypeToken<LinkedHashMap<String, String>>() {
            }.getType();
            LinkedHashMap<String, String> map = gson.fromJson(body, type);
            String cloudId = map.get(ParamConstants.F_CLOUDID);
            String schema = map.get(ParamConstants.F_REPRESENTATIONNAME);

            if (cloudId != null && schema != null) {
                if (!cloudId.isEmpty() && !schema.isEmpty()) {
                    try {
                        solrDao.removeRepresentation(cloudId, schema);
                    } catch (SolrServerException | IOException ex) {
                        LOGGER.error("Cannot remove representation from solr", ex);
                    }
                } else {
                    LOGGER.error("Empty parameters in message body: {}: {}, {}: {}", ParamConstants.F_CLOUDID, cloudId,
                        ParamConstants.F_REPRESENTATIONNAME, schema);
                }
            } else {
                LOGGER.error("Empty parameters in message body: {}: {}, {}: {}", ParamConstants.F_CLOUDID, cloudId,
                    ParamConstants.F_REPRESENTATIONNAME, schema);
            }
        } else {
            LOGGER.error("Received message has no body");
        }
    }
}
