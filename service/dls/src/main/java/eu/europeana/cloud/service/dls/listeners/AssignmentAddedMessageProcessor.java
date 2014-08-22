package eu.europeana.cloud.service.dls.listeners;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import eu.europeana.cloud.common.web.ParamConstants;
import eu.europeana.cloud.common.model.CompoundDataSetId;
import eu.europeana.cloud.service.dls.solr.SolrDAO;
import eu.europeana.cloud.service.dls.solr.exception.SolrDocumentNotFoundException;
import eu.europeana.cloud.service.mcs.messages.AddAssignmentMessage;
import java.io.IOException;
import org.apache.commons.lang.StringUtils;
import org.apache.solr.client.solrj.SolrServerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Processor that processes messages about adding an assignment of
 * {@link eu.europeana.cloud.common.model.Representation representation} version
 * a certain {@link eu.europeana.cloud.common.model.DataSet data set}.
 * 
 * It processes messages with <code>datasets.assignments.delete</code> routing
 * key. Message text should be Json including {@link CompoundDataSetId} object
 * and property containing id of representation version.
 * 
 * After processing properly formed message, processor calls
 * {@link eu.europeana.cloud.service.dls.solr.SolrDAO#addAssignment(String, CompoundDataSetId)}
 * so that Solr index is updated (
 * {@link eu.europeana.cloud.common.model.DataSet data set} will have new
 * version assigned in updated index).
 * 
 * If message is malformed, information about error is logged and no call to
 * {@link eu.europeana.cloud.service.dls.solr.SolrDAO} is performed. If call to
 * {@link eu.europeana.cloud.service.dls.solr.SolrDAO} fails, an information is
 * also logged.
 * 
 * Messages for this processor are produced by
 * <code>eu.europeana.cloud.service.mcs.persistent.SolrRepresentationIndexer.addAssignment(String, CompoundDataSetId)}</code>
 * method in MCS.
 */
@Component
public class AssignmentAddedMessageProcessor implements
	MessageProcessor<AddAssignmentMessage> {

    private static final Logger LOGGER = LoggerFactory
	    .getLogger(AssignmentAddedMessageProcessor.class);

    @Autowired
    SolrDAO solrDao;

    private static final Gson gson = new Gson();

    @Override
    public void processMessage(AddAssignmentMessage message) {
       String messageText = message.getPayload();
       
        if (messageText == null || messageText.isEmpty()) {
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

        JsonElement dsJson = jo.get(ParamConstants.F_DATASET_PROVIDER_ID);
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
