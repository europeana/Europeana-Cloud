package eu.europeana.cloud.service.dls.listeners;

import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
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

        JsonObject jo = gson.fromJson(messageText, JsonElement.class).getAsJsonObject();

        String cloudId = null;
        JsonElement cloudIdJson = jo.get(ParamConstants.P_CLOUDID);
        if (cloudIdJson != null && !cloudIdJson.isJsonNull()) {
            cloudId = cloudIdJson.getAsString();
        }
        if (StringUtils.isBlank(cloudId)) {
            LOGGER.error("Required parameter version is empty.");
            return;
        }

        String representationName = null;
        JsonElement representationNameJson = jo.get(ParamConstants.P_REPRESENTATIONNAME);
        if (representationNameJson != null && !representationNameJson.isJsonNull()) {
            representationName = representationNameJson.getAsString();
        }
        if (StringUtils.isBlank(representationName)) {
            LOGGER.error("Required parameter representationName is empty.");
            return;
        }

        try {
            solrDao.removeRepresentation(cloudId, representationName);
        } catch (SolrServerException | IOException ex) {
            LOGGER.error("Cannot remove representation from solr.", ex);
        }
    }
}
