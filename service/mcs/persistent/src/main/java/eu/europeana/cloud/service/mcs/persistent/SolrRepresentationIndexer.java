package eu.europeana.cloud.service.mcs.persistent;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import eu.europeana.cloud.common.model.CompoundDataSetId;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.web.ParamConstants;
import eu.europeana.cloud.service.mcs.kafka.ProducerWrapper;
import eu.europeana.cloud.service.mcs.messages.AddAssignmentMessage;
import eu.europeana.cloud.service.mcs.messages.InsertRepresentationMessage;
import eu.europeana.cloud.service.mcs.messages.InsertRepresentationPersistentMessage;
import eu.europeana.cloud.service.mcs.messages.RemoveAssignmentMessage;
import eu.europeana.cloud.service.mcs.messages.RemoveAssignmentsFromDataSetMessage;
import eu.europeana.cloud.service.mcs.messages.RemoveRecordRepresentationsMessage;
import eu.europeana.cloud.service.mcs.messages.RemoveRepresentationMessage;
import eu.europeana.cloud.service.mcs.messages.RemoveRepresentationVersionMessage;
import eu.europeana.cloud.service.mcs.persistent.cassandra.CassandraDataSetDAO;
import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import org.apache.commons.lang.SerializationUtils;
import static org.apache.commons.lang.SerializationUtils.serialize;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Receives information about modifications in record representations and their
 * data set assignments and sends them to index. Uses asynchronous queue to
 * communicate with Data Lookup Service which is responsible for Solr index
 * updates.
 */
@Component
public class SolrRepresentationIndexer {

    private static final Logger LOGGER = LoggerFactory
	    .getLogger(SolrRepresentationIndexer.class);

    @Autowired
    private CassandraDataSetDAO cassandraDataSetDAO;

    @Autowired(required = true)
    private ProducerWrapper producerWrapper;

    private final Gson gson = new GsonBuilder().setDateFormat(
	    "yyyy-MM-dd'T'HH:mm:ss.SSSZZ").create();

    /**
     * Indexes representation version (new or updated). If inserted
     * representation version is persistent, there there might be some data set
     * assignment changes involved (in cases where representation in latest
     * persistent version is assigned to data set) - in this case, such
     * reassignment is also done.
     * 
     * @param representation
     *            representation version.
     * @param partitionKey
     *            using to route message to different partitions.
     */
    public void insertRepresentation(Representation representation,
	    int partitionKey) {

	if (!representation.isPersistent()) {
	    producerWrapper
		    .send(partitionKey,
			    serialize(new InsertRepresentationMessage(
				    prepareInsertRepresentationMessage(representation))));

	} else {
	    Collection<CompoundDataSetId> dataSetIds = cassandraDataSetDAO
		    .getDataSetAssignments(representation.getCloudId(),
			    representation.getRepresentationName(), null);
	    producerWrapper.send(
		    partitionKey,
		    serialize(new InsertRepresentationPersistentMessage(
			    prepareInsertPersistentRepresentationMessage(
				    representation, dataSetIds))));
	}
    }

    private String prepareInsertRepresentationMessage(
	    Representation representation) {
	return gson.toJson(representation);
    }

    private String prepareInsertPersistentRepresentationMessage(Representation representation,
            Collection<CompoundDataSetId> dataSetIds) {
        HashMap<String, Object> map = new LinkedHashMap<>();
        map.put(ParamConstants.F_REPRESENTATION, representation);
        map.put(ParamConstants.F_DATASETS, dataSetIds);
        return gson.toJson(map);
    }

    /**
     * Removes representation version from index. This method should be invoked
     * only for temporary representation versions (although no verification is
     * made in this method) - because no dataset reassignment is performed. *
     * 
     * @param versionId
     *            representation version id
     * 
     * @param partitionKey
     *            using to route message to different partitions.
     */
    public void removeRepresentationVersion(String versionId, int partitionKey) {
	producerWrapper.send(partitionKey,
		serialize(new RemoveRepresentationVersionMessage(versionId)));
    }

    /**
     * Removes all representation's versions from index.
     * 
     * @param cloudId
     *            record id
     * @param representationName
     *            representation's schema
     * @param partitionKey
     *            using to route message to different partitions.
     */
    public void removeRepresentation(String cloudId, String representationName,
	    int partitionKey) {
	producerWrapper.send(
		partitionKey,
		serialize(new RemoveRepresentationMessage(
			prepareRemoveRepresentationMsg(cloudId,
				representationName))));
    }

    private String prepareRemoveRepresentationMsg(String cloudId,
	    String representationName) {
	JsonObject jo = new JsonObject();
	jo.addProperty(ParamConstants.P_CLOUDID, cloudId);
	jo.addProperty(ParamConstants.P_REPRESENTATIONNAME, representationName);
	return jo.toString();
    }

    /**
     * Removes all record's representations with all their versions from index.
     * 
     * @param cloudId
     *            record id
     * @param partitionKey
     *            using to route message to different partitions.
     */
    public void removeRecordRepresentations(String cloudId, int partitionKey) {
	producerWrapper.send(partitionKey,
		serialize(new RemoveRecordRepresentationsMessage(cloudId)));
    }

    /**
     * Removes assignment between data set and representation (regardless its
     * version).
     * 
     * @param cloudId
     *            record id
     * @param representationName
     *            representation's schema
     * @param dataSetId
     *            dataset id with owner's (provider's) id.
     * @param partitionKey
     *            using to route message to different partitions.
     */
    public void removeAssignment(String cloudId, String representationName,
	    CompoundDataSetId dataSetId, int partitionKey) {
	producerWrapper.send(
		partitionKey,
		serialize(new RemoveAssignmentMessage(
			prepareRemoveAssignmentMessage(cloudId,
				representationName, dataSetId))));
    }

    private String prepareRemoveAssignmentMessage(String cloudId,
	    String representationName, CompoundDataSetId dataSetId) {
	JsonObject jo = prepareCompoundDataSetIdJson(dataSetId);
	jo.addProperty(ParamConstants.P_CLOUDID, cloudId);
	jo.addProperty(ParamConstants.P_REPRESENTATIONNAME, representationName);
	return jo.toString();
    }

    /**
     * Adds assignment between data set and a representation version.
     * 
     * @param versionId
     *            representation version id.
     * @param dataSetId
     *            dataset id with owner's (provider's) id.
     * @param partitionKey
     *            using to route message to different partitions.
     */
    public void addAssignment(String versionId, CompoundDataSetId dataSetId,
	    int partitionKey) {
	producerWrapper.send(partitionKey, serialize(new AddAssignmentMessage(
		prepareAddAssignmentMsg(versionId, dataSetId))));
    }

    private String prepareAddAssignmentMsg(String versionId,
	    CompoundDataSetId dataSetId) {
	JsonObject jo = prepareCompoundDataSetIdJson(dataSetId);
	jo.addProperty(ParamConstants.P_VER, versionId);
	return jo.toString();
    }

    /**
     * Removes data set assignments from ALL representation. This method is to
     * be used if whole data set is removed.
     * 
     * @param compoundDataSetId
     *            dataset id with owner's (provider's) id.
     * @param partitionKey
     *            using to route message to different partitions.
     */
    public void removeAssignmentsFromDataSet(
	    CompoundDataSetId compoundDataSetId, int partitionKey) {
	producerWrapper.send(
		partitionKey,
		serialize(new RemoveAssignmentsFromDataSetMessage(gson
			.toJson(compoundDataSetId))));
    }

    private JsonObject prepareCompoundDataSetIdJson(CompoundDataSetId dataSetId) {
	JsonElement elem = gson.toJsonTree(dataSetId, CompoundDataSetId.class);
	JsonObject jo = new JsonObject();
	jo.add(ParamConstants.F_DATASET_PROVIDER_ID, elem);
	return jo;
    }

    // TODO change
    private byte[] serialize(Serializable in) {
	return SerializationUtils.serialize(in);
    }
}
