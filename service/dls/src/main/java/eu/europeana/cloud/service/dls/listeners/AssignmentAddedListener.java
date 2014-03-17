package eu.europeana.cloud.service.dls.listeners;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import eu.europeana.cloud.common.web.ParamConstants;
import eu.europeana.cloud.service.dls.solr.CompoundDataSetId;
import eu.europeana.cloud.service.dls.solr.SolrDAO;
import eu.europeana.cloud.service.dls.solr.exception.SolrDocumentNotFoundException;
import java.io.IOException;
import org.apache.commons.lang.StringUtils;
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
public class AssignmentAddedListener implements MessageListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(AssignmentAddedListener.class);

    @Autowired
    SolrDAO solrDao;

    private static final Gson gson = new Gson();

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

        String version = null;
        JsonElement versionJson = jo.get(ParamConstants.P_VER);
        if (versionJson != null && !versionJson.isJsonNull()) {
            version = versionJson.getAsString();
        }
        if (StringUtils.isBlank(version)) {
            LOGGER.error("Required parameter version is empty.");
            return;
        }

        JsonElement dsJson = jo.get("compoundDataSetId");
        CompoundDataSetId compoundDataSetId = gson.fromJson(dsJson, CompoundDataSetId.class);
        if (compoundDataSetId == null) {
            LOGGER.error("Required parameter CompoundDataSetId is null.");
            return;
        }
        if (StringUtils.isBlank(compoundDataSetId.getDataSetId())) {
            LOGGER.error("Data set id is empty.");
            return;
        }
        if (StringUtils.isBlank(compoundDataSetId.getDataSetProviderId())) {
            LOGGER.error("Provider id is empty.");
            return;
        }

        try {
            solrDao.addAssignment(version, compoundDataSetId);
        } catch (SolrServerException | IOException | SolrDocumentNotFoundException ex) {
            LOGGER.error("Cannot add assignment to solr.", ex);
        }

    }

}
