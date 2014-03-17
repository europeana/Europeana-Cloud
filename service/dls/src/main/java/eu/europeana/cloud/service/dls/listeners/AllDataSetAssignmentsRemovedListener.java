package eu.europeana.cloud.service.dls.listeners;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import eu.europeana.cloud.common.model.CompoundDataSetId;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.service.dls.solr.SolrDAO;
import java.io.IOException;
import org.apache.commons.lang.StringUtils;
import org.apache.solr.client.solrj.SolrServerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 *
 */
@Component
public class AllDataSetAssignmentsRemovedListener implements MessageListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(AllDataSetAssignmentsRemovedListener.class);

    @Autowired
    SolrDAO solrDAO;

    private final Gson gson = new Gson();

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

        CompoundDataSetId compoundDataSetId = gson.fromJson(messageText, CompoundDataSetId.class);
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
            solrDAO.removeAssignmentFromDataSet(compoundDataSetId);
        } catch (IOException | SolrServerException ex) {
            LOGGER.error("Cannot remove assignments from data set in solr.", ex);
        }
    }

}
