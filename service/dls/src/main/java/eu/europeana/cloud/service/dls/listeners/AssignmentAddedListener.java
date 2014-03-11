package eu.europeana.cloud.service.dls.listeners;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import eu.europeana.cloud.common.web.ParamConstants;
import eu.europeana.cloud.service.dls.solr.CompoundDataSetId;
import eu.europeana.cloud.service.dls.solr.SolrDAO;
import eu.europeana.cloud.service.dls.solr.exception.SolrDocumentNotFoundException;
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
public class AssignmentAddedListener implements MessageListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(AssignmentAddedListener.class);

    @Autowired
    SolrDAO solrDao;

    private static final Gson gson = new Gson();


    @Override
    public void onMessage(Message message) {
        byte[] body = message.getBody();
        if (body != null) {
            String json = new String(body);
            if (!json.isEmpty()) {
                JsonObject jo = gson.fromJson(json, JsonElement.class).getAsJsonObject();
                JsonElement versionJson = jo.get(ParamConstants.P_VER);
                JsonElement dsJson = jo.get("compoundDataSetId");
                if (versionJson != null && dsJson != null) {
                    CompoundDataSetId dataSetId = gson.fromJson(dsJson, CompoundDataSetId.class);
                    String version = versionJson.getAsString();
                    if (!version.isEmpty() && !dataSetId.getDataSetId().isEmpty()
                            && !dataSetId.getDataSetProviderId().isEmpty()) {
                        try {
                            solrDao.addAssignment(version, dataSetId);
                        } catch (SolrServerException | IOException | SolrDocumentNotFoundException ex) {
                            LOGGER.error("Cannot add assignment to solr", ex);
                        }
                    } else {
                        LOGGER.error("Required parameters missing. Version: {}, DataProviderId: {}, DataSetId: {}",
                            version, dataSetId.getDataSetProviderId(), dataSetId.getDataSetId());
                    }
                }
            } else {
                LOGGER.error("Message has empty body");
            }
        } else {
            LOGGER.error("Message has null body");
        }
    }

}
