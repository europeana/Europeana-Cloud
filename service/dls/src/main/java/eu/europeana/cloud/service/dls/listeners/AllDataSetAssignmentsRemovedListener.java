package eu.europeana.cloud.service.dls.listeners;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import eu.europeana.cloud.service.dls.solr.CompoundDataSetId;
import eu.europeana.cloud.service.dls.solr.SolrDAO;
import java.io.IOException;
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
        try {
            CompoundDataSetId dataSetId = getDataSetIdFromMessage(message);
            if (dataSetId != null) {
                solrDAO.removeAssignmentFromDataSet(dataSetId);
            }
        } catch (SolrServerException | IOException ex) {
            LOGGER.error("Cannot remove assignments from data set in solr", ex);
        }
    }


    private CompoundDataSetId getDataSetIdFromMessage(Message message) {
        byte[] body = message.getBody();
        if (body == null) {
            LOGGER.error("Message has null body");
            return null;
        }

        String json = new String(body);
        if (json.isEmpty()) {
            LOGGER.error("Message has empty body");
            return null;
        }

        JsonObject jo = gson.fromJson(json, JsonElement.class).getAsJsonObject();
        JsonElement dsJson = jo.get("compoundDataSetId");
        if (dsJson == null) {
            LOGGER.error("Required parameters missing.");
            return null;
        }

        CompoundDataSetId dataSetId = gson.fromJson(dsJson, CompoundDataSetId.class);
        if (dataSetId.getDataSetId() == null || dataSetId.getDataSetProviderId() == null) {
            LOGGER.error("Required parameters missing. DataProviderId: {}, DataSetId: {}",
                dataSetId.getDataSetProviderId(), dataSetId.getDataSetId());
            return null;
        }

        if (dataSetId.getDataSetId().isEmpty() || dataSetId.getDataSetProviderId().isEmpty()) {
            LOGGER.error("Required parameters missing. DataProviderId: {}, DataSetId: {}",
                dataSetId.getDataSetProviderId(), dataSetId.getDataSetId());
            return null;
        }
        return dataSetId;
    }

}
