package eu.europeana.cloud.service.dls.listeners;

import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import eu.europeana.cloud.common.web.ParamConstants;
import eu.europeana.cloud.service.dls.solr.SolrDAO;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.LinkedHashMap;
import org.apache.commons.lang.StringUtils;
import org.apache.solr.client.solrj.SolrServerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Listener class which reacts on messages about deletion of record's
 * representation.
 */
@Component
public class RepresentationRemovedListener implements MessageListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(RepresentationRemovedListener.class);

    @Autowired
    SolrDAO solrDao;

    private final Gson gson = new Gson();

    /**
     * Reacts on message about representation removal and removes all versions
     * of requested representation from index. Message body should contain cloud
     * identifier and representationName of the representation.
     *
     * @param message
     */
    @Override
    public void onMessage(Message message) {

        byte[] messageBytes = message.getBody();
        if (messageBytes == null) {
            LOGGER.error("Message has null body.");
            return;
        }

        String messageText = new String(message.getBody());
        if (messageText.isEmpty()) {
            LOGGER.error("Message has empty body.");
            return;
        }

        Type type = new TypeToken<LinkedHashMap<String, String>>() {
        }.getType();
        LinkedHashMap<String, String> map = gson.fromJson(messageText, type);

        String cloudId = map.get(ParamConstants.F_CLOUDID);
        if (StringUtils.isBlank(cloudId)) {
            LOGGER.error("Required parameter cloud id is empty.");
            return;
        }

        String representationName = map.get(ParamConstants.F_REPRESENTATIONNAME);
        if (StringUtils.isBlank(representationName)) {
            LOGGER.error("Required parameter representation name is empty.");
            return;
        }
        try {
            solrDao.removeRepresentation(cloudId, representationName);
        } catch (SolrServerException | IOException ex) {
            LOGGER.error("Cannot remove representation from solr.", ex);
        }
    }
}
