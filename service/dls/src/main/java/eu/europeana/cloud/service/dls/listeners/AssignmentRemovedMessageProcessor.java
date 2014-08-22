package eu.europeana.cloud.service.dls.listeners;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import eu.europeana.cloud.common.web.ParamConstants;
import eu.europeana.cloud.common.model.CompoundDataSetId;
import eu.europeana.cloud.service.dls.solr.SolrDAO;
import eu.europeana.cloud.service.dls.solr.exception.SolrDocumentNotFoundException;
import eu.europeana.cloud.service.mcs.messages.RemoveAssignmentMessage;
import java.io.IOException;
import java.util.Collections;
import org.apache.commons.lang.StringUtils;
import org.apache.solr.client.solrj.SolrServerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Processor that processes messages about removing an assignment of
 * {@link eu.europeana.cloud.common.model.Representation representation} of
 * certain representation name to a certain
 * {@link eu.europeana.cloud.common.model.DataSet data set}.
 * 
 * It processes messages with <code>datasets.assignments.delete</code> routing
 * key. Message text should be Json including {@link CompoundDataSetId} object,
 * a property containing representation name and a property containing cloudId
 * of the {@link eu.europeana.cloud.common.model.Record record}. We need the
 * cloudId to uniquely identify the
 * {@link eu.europeana.cloud.common.model.Representation representation}
 * versions of the provided representation name, as no version id is included.
 * 
 * After processing properly formed message, processor calls
 * {@link eu.europeana.cloud.service.dls.solr.SolrDAO#removeAssignment(String, String, Collection)
 * SolrDAO.removeAssignment(String, String,
 * Collection&lt;CompoundDataSetId&gt;)} so that Solr index is updated (
 * {@link eu.europeana.cloud.common.model.DataSet data set} will have no
 * versions of this {@link eu.europeana.cloud.common.model.Representation
 * representation} assigned in updated index).
 * 
 * If message is malformed, information about error is logged and no call to
 * {@link eu.europeana.cloud.service.dls.solr.SolrDAO} is performed. If call to
 * {@link eu.europeana.cloud.service.dls.solr.SolrDAO} fails, an information is
 * also logged.
 * 
 * Messages for this processor are produced by
 * <code>eu.europeana.cloud.service.mcs.persistent.SolrRepresentationIndexer.removeAssignment(String, String, CompoundDataSetId)}</code>
 * method in MCS.
 */
@Component
public class AssignmentRemovedMessageProcessor implements
	MessageProcessor<RemoveAssignmentMessage> {

    private static final Logger LOGGER = LoggerFactory
	    .getLogger(AssignmentRemovedMessageProcessor.class);

    @Autowired
    SolrDAO solrDao;

    private static final Gson gson = new Gson();

    @Override
    public void processMessage(RemoveAssignmentMessage message) {
        String messageText = message.getPayload();

        if (messageText == null || messageText.isEmpty()) {
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
            solrDao.removeAssignment(cloudId, representationName, Collections.singletonList(compoundDataSetId));
        } catch (SolrServerException | IOException | SolrDocumentNotFoundException ex) {
            LOGGER.error("Cannot remove assignment from solr", ex);
        }
    }
}
