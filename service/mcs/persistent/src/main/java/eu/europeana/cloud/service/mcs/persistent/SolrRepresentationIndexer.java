package eu.europeana.cloud.service.mcs.persistent;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import eu.europeana.cloud.common.model.CompoundDataSetId;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.web.ParamConstants;
import eu.europeana.cloud.service.mcs.kafka.ProducerWrapper;
import eu.europeana.cloud.service.mcs.persistent.cassandra.CassandraDataSetDAO;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
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
    private RabbitTemplate template;

    // @Autowired
    // private ProducerWrapper producerWrapper;

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
     */
    public void insertRepresentation(Representation representation) {

	if (!representation.isPersistent()) {
	    template.convertAndSend("records.representations.versions.add",
		    prepareInsertRepresentationMessage(representation));
	    // producerWrapper.send("1", gson.toJson(new InsertRepresentation(
	    // prepareInsertRepresentationMessage(representation))));

	} else {
	    Collection<CompoundDataSetId> dataSetIds = cassandraDataSetDAO
		    .getDataSetAssignments(representation.getCloudId(),
			    representation.getRepresentationName(), null);

	    template.convertAndSend(
		    "records.representations.versions.addPersistent",
		    prepareInsertPersistentRepresentationMessage(
			    representation, dataSetIds));
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
     */
    public void removeRepresentationVersion(String versionId) {
	template.convertAndSend(
		"records.representations.versions.deleteVersion", versionId);
    }

    /**
     * Removes all representation's versions from index.
     * 
     * @param cloudId
     *            record id
     * @param representationName
     *            representation's schema
     */
    public void removeRepresentation(String cloudId, String representationName) {
	template.convertAndSend("records.representations.delete",
		prepareRemoveRepresentationMsg(cloudId, representationName));
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
     */
    public void removeRecordRepresentations(String cloudId) {
	template.convertAndSend("records.representations.deleteAll", cloudId);
    }

    /**
     * Removes assignment between data set and representation (regardless its
     * version).
     * 
     * @param cloudId
     * @param representationName
     * @param dataSetId
     */
    public void removeAssignment(String cloudId, String representationName,
	    CompoundDataSetId dataSetId) {
	template.convertAndSend(
		"datasets.assignments.delete",
		prepareRemoveAssignmentMessage(cloudId, representationName,
			dataSetId));
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
     */
    public void addAssignment(String versionId, CompoundDataSetId dataSetId) {
	template.convertAndSend("datasets.assignments.add",
		prepareAddAssignmentMsg(versionId, dataSetId));
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
     */
    public void removeAssignmentsFromDataSet(CompoundDataSetId compoundDataSetId) {
	template.convertAndSend("datasets.assignments.deleteAll",
		gson.toJson(compoundDataSetId));
    }

    private JsonObject prepareCompoundDataSetIdJson(CompoundDataSetId dataSetId) {
	JsonElement elem = gson.toJsonTree(dataSetId, CompoundDataSetId.class);
	JsonObject jo = new JsonObject();
	jo.add(ParamConstants.F_DATASET_PROVIDER_ID, elem);
	return jo;
    }
}
