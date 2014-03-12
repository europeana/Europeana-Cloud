package eu.europeana.cloud.service.mcs.persistent;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import eu.europeana.cloud.service.mcs.persistent.util.CompoundDataSetId;
import eu.europeana.cloud.service.mcs.persistent.cassandra.CassandraDataSetDAO;
import eu.europeana.cloud.service.mcs.persistent.solr.SolrDAO;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.web.ParamConstants;
import eu.europeana.cloud.service.mcs.persistent.exception.SolrDocumentNotFoundException;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import org.apache.solr.client.solrj.SolrServerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/**
 * Receives information about modifications in record representations and their
 * data set assignments and sends them to index. All public methods are
 * asynchronous.
 *
 * This class requires task executor in spring configuration with id:
 * solrIndexerExecutor.
 *
 * In future, implementation should be changed to use more reliable (probably
 * persistent) queueing technology (i.e. jms or RabbitMQ).
 */
@Component
@Async("solrIndexerExecutor")
public class SolrRepresentationIndexer {

    private static final Logger LOGGER = LoggerFactory.getLogger(SolrRepresentationIndexer.class);

    @Autowired
    private SolrDAO solrDAO;

    @Autowired
    private CassandraDataSetDAO cassandraDataSetDAO;

    @Autowired
    private RabbitTemplate template;

    private final Gson gson = new GsonBuilder()
            .setDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZZ")
            .create();

    /**
     * Indexes representation version (new or updated). If inserted
     * representation version is persistent, there there might be some data set
     * assignment changes involved (in cases where representation in latest
     * persistent version is assigned to data set) - in this case, such
     * reassignment is also done.
     *
     * @param representation representation version.
     */
    public void insertRepresentation(Representation representation) {

        try {
            if (!representation.isPersistent()) {
                solrDAO.insertRepresentation(representation, null);
                template.convertAndSend("records.representations.versions.add",
                        prepareInsertRepresentationMessage(representation));
            } else {
                Collection<CompoundDataSetId> dataSetIds = cassandraDataSetDAO.getDataSetAssignments(
                        representation.getCloudId(), representation.getRepresentationName(), null);
                solrDAO.insertRepresentation(representation, dataSetIds);
                template.convertAndSend("records.representations.versions.addPersistent", prepareInsertPersistentRepresentationMessage(representation, dataSetIds));
            }
        } catch (IOException | SolrServerException ex) {
            LOGGER.error("Cannot insert representation into solr", ex);
        }
    }

    private String prepareInsertRepresentationMessage(Representation representation) {
        return gson.toJson(representation);
    }

    private String prepareInsertPersistentRepresentationMessage(Representation representation, Collection<CompoundDataSetId> dataSetIds) {
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
     * @param versionId representation version id
     */
    public void removeRepresentationVersion(String versionId) {
        try {
            solrDAO.removeRepresentationVersion(versionId);
            template.convertAndSend("records.representations.versions.deleteVersion", versionId);
        } catch (SolrServerException | IOException ex) {
            LOGGER.error("Cannot remove representation from solr", ex);
        }
    }

    /**
     * Removes all representation's versions from index.
     *
     * @param cloudId record id
     * @param representationName representation's schema
     */
    public void removeRepresentation(String cloudId, String representationName) {
        try {
            solrDAO.removeRepresentation(cloudId, representationName);
            template.convertAndSend("records.representations.delete",
                    prepareRemoveRepresentationMsg(cloudId, representationName));
        } catch (SolrServerException | IOException ex) {
            LOGGER.error("Cannot remove representation from solr", ex);
        }
    }

    private String prepareRemoveRepresentationMsg(String cloudId, String schema) {
        HashMap<String, String> map = new LinkedHashMap<>();
        map.put(ParamConstants.F_CLOUDID, cloudId);
        map.put(ParamConstants.F_REPRESENTATIONNAME, schema);
        return gson.toJson(map);
    }

    /**
     * Removes all record's representations with all their versions from index.
     *
     * @param cloudId
     */
    public void removeRecordRepresentations(String cloudId) {
        try {
            solrDAO.removeRecordRepresentation(cloudId);
            template.convertAndSend("records.representations.deleteAll", cloudId);
        } catch (SolrServerException | IOException ex) {
            LOGGER.error("Cannot remove representation from solr", ex);
        }
    }

    /**
     * Removes assignment between data set and representation (regardless its
     * version).
     *
     * @param cloudId
     * @param representationName
     * @param dataSetId
     */
    public void removeAssignment(String cloudId, String representationName, CompoundDataSetId dataSetId) {
        try {
            solrDAO.removeAssignment(cloudId, representationName, Collections.singletonList(dataSetId));
            template.convertAndSend("datasets.assignments.delete",
                prepareRemoveAssginmentMessage(cloudId, representationName, dataSetId));
        } catch (SolrServerException | IOException | SolrDocumentNotFoundException ex) {
            LOGGER.error("Cannot remove assignment from solr", ex);
        }
    }

    private String prepareRemoveAssginmentMessage(String cloudId, String representationName, CompoundDataSetId dataSetId) {
        JsonObject jo = prepareCompoundDataSetIdJson(dataSetId);
        jo.addProperty(ParamConstants.P_CLOUDID, cloudId);
        jo.addProperty(ParamConstants.P_REPRESENTATIONNAME, representationName);
        return jo.toString();
    }

    /**
     * Adds assignment between data set and a representation version.
     *
     * @param versionId representation version id.
     * @param dataSetId dataset id with owner's (provider's) id.
     */
    public void addAssignment(String versionId, CompoundDataSetId dataSetId) {
        try {
            solrDAO.addAssignment(versionId, dataSetId);
            template.convertAndSend("datasets.assignments.add", prepareAddAssignmentMsg(versionId, dataSetId));
        } catch (SolrServerException | IOException | SolrDocumentNotFoundException ex) {
            LOGGER.error("Cannot add assignment to solr", ex);
        }
    }

    private String prepareAddAssignmentMsg(String versionId, CompoundDataSetId dataSetId) {
        JsonObject jo = prepareCompoundDataSetIdJson(dataSetId);
        jo.addProperty(ParamConstants.P_VER, versionId);
        return jo.toString();
    }

    /**
     * Removes data set assignments from ALL representation. This method is to
     * be used if whole data set is removed.
     *
     * @param dataSetId dataset id with owner's (provider's) id.
     */
    public void removeAssignmentsFromDataSet(CompoundDataSetId dataSetId) {
        try {
            solrDAO.removeAssignmentFromDataSet(dataSetId);
            template.convertAndSend("datasets.assignments.deleteAll", prepareCompoundDataSetIdJson(dataSetId)
                    .toString());
        } catch (SolrServerException | IOException ex) {
            LOGGER.error("Cannot remove assignments from data set in solr", ex);
        }
    }


    private JsonObject prepareCompoundDataSetIdJson(CompoundDataSetId dataSetId) {
        JsonElement elem = gson.toJsonTree(dataSetId, CompoundDataSetId.class);
        JsonObject jo = new JsonObject();
        jo.add("compoundDataSetId", elem);
        return jo;
    }
}
