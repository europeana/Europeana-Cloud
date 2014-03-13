package eu.europeana.cloud.service.dls.listeners;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.web.ParamConstants;
import eu.europeana.cloud.service.dls.solr.CompoundDataSetId;
import eu.europeana.cloud.service.dls.solr.SolrDAO;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Collection;
import java.util.HashMap;
import org.apache.solr.client.solrj.SolrServerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Listener class which reacts on messages about
 */
@Component
public class RepresentationVersionAddedPersistentListener implements MessageListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(RepresentationVersionAddedPersistentListener.class);
    private static final Gson gson = new GsonBuilder()
            .setDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZZ")
            .create();

    @Autowired
    SolrDAO solrDao;

    /**
     *
     * @param message
     */
    @Override
    public void onMessage(Message message) {
        String messageBody = message.getBody().toString();
        if (messageBody == null) {
            LOGGER.error("Message has null body");
            return;
        }
        if (messageBody.equals("")) {
            LOGGER.error("Message has empty body");
        }

        JsonElement jsonElem = gson.fromJson(messageBody, JsonElement.class);
        JsonObject jsonObject = jsonElem.getAsJsonObject();

        JsonElement jsonRepresentation = jsonObject.get(ParamConstants.F_REPRESENTATION);
        Representation representation = gson.fromJson(jsonRepresentation, Representation.class);

        Type dataSetIdsType = new TypeToken<Collection<CompoundDataSetId>>() {
        }.getType();
        JsonElement jsonDataSetIds = jsonObject.get(ParamConstants.F_DATASETS);
        Collection<CompoundDataSetId> dataSetIds = gson.fromJson(jsonDataSetIds, dataSetIdsType);

        try {
            solrDao.insertRepresentation(representation, dataSetIds);
        } catch (IOException | SolrServerException ex) {
            LOGGER.error("Cannot insert persistent representation into solr", ex);
        }
    }
}
