package eu.europeana.cloud.service.mcs.persistent;

import eu.europeana.cloud.service.mcs.persistent.util.CompoundDataSetId;
import eu.europeana.cloud.service.mcs.persistent.cassandra.CassandraDataSetDAO;
import eu.europeana.cloud.service.mcs.persistent.solr.SolrDAO;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.service.mcs.persistent.exception.SolrDocumentNotFoundException;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import org.apache.solr.client.solrj.SolrServerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/**
 * Receives information about modifications in record representations and their data set assignments and sends them to
 * index. All public methods are asynchronous.
 * 
 * This class requires task executor in spring configuration with id: solrIndexerExecutor.
 * 
 * In future, implementation should be changed to use more reliable (probably persistent) queuing technology (i.e. jms
 * or RabbitMQ).
 */
@Component
@Async("solrIndexerExecutor")
public class SolrRepresentationIndexer {

    private static final Logger LOGGER = LoggerFactory.getLogger(SolrRepresentationIndexer.class);

    @Autowired
    private SolrDAO solrDAO;

    @Autowired
    private CassandraDataSetDAO cassandraDataSetDAO;


    /**
     * Indexes representation version (new or updated). If inserted representation version is persistent, there there
     * might be some data set assignment changes involved (in cases where representation in latest persistent version is
     * assigned to data set) - in this case, such reassignment is also done.
     * 
     * @param representation
     *            representation version.
     */
    public void insertRepresentation(Representation representation) {
        try {
            if (!representation.isPersistent()) {
                solrDAO.insertRepresentation(representation, null);
            } else {
                Collection<CompoundDataSetId> dataSetIds = cassandraDataSetDAO.getDataSetAssignments(
                    representation.getRecordId(), representation.getSchema(), null);
                solrDAO.removeAssignment(representation.getRecordId(), representation.getSchema(), dataSetIds);
                solrDAO.insertRepresentation(representation, dataSetIds);
            }
        } catch (IOException | SolrDocumentNotFoundException | SolrServerException ex) {
            LOGGER.error("Cannot insert representation into solr", ex);
        }
    }


    /**
     * Removes representation version from index. This method should be invoked only for temporary representation
     * versions (although no verification is made in this method) - because no dataset reassignment is performed. *
     * 
     * @param versionId
     *            representation version id
     */
    public void removeRepresentationVersion(String versionId) {
        try {
            solrDAO.removeRepresentationVersion(versionId);
        } catch (SolrServerException | IOException ex) {
            LOGGER.error("Cannot remove representation from solr", ex);
        }
    }


    /**
     * Removes all representation's versions from index.
     * 
     * @param cloudId
     *            record id
     * @param schema
     *            represenation's schema
     */
    public void removeRepresentation(String cloudId, String schema) {
        try {
            solrDAO.removeRepresentation(cloudId, schema);
        } catch (SolrServerException | IOException ex) {
            LOGGER.error("Cannot remove representation from solr", ex);
        }
    }


    /**
     * Removes all record's representations with all their versions from index.
     * 
     * @param cloudId
     */
    public void removeRecordRepresentations(String cloudId) {
        try {
            solrDAO.removeRecordRepresentation(cloudId);
        } catch (SolrServerException | IOException ex) {
            LOGGER.error("Cannot remove representation from solr", ex);
        }
    }


    /**
     * Removes assignment between data set and representation (regardless its version).
     * 
     * @param recordId
     * @param schema
     * @param dataSetId
     */
    public void removeAssignment(String recordId, String schema, CompoundDataSetId dataSetId) {
        try {
            solrDAO.removeAssignment(recordId, schema, Collections.singletonList(dataSetId));
        } catch (SolrServerException | IOException | SolrDocumentNotFoundException ex) {
            LOGGER.error("Cannot remove assignment from solr", ex);
        }
    }


    /**
     * Adds assignment between data set and a represenation version.
     * 
     * @param versionId
     *            representation version id.
     * @param dataSetId
     *            dataset id with owner's (provider's) id.
     */
    public void addAssignment(String versionId, CompoundDataSetId dataSetId) {
        try {
            solrDAO.addAssignment(versionId, dataSetId);
        } catch (SolrServerException | IOException | SolrDocumentNotFoundException ex) {
            LOGGER.error("Cannot add assignment to solr", ex);
        }
    }


    /**
     * Removes data set assignments from ALL representation. This method is to be used if whole data set is removed.
     * 
     * @param dataSetId
     *            dataset id with owner's (provider's) id.
     */
    public void removeAssignmentsFromDataSet(CompoundDataSetId dataSetId) {
        try {
            solrDAO.removeAssignmentFromDataSet(dataSetId);
        } catch (SolrServerException | IOException ex) {
            LOGGER.error("Cannot remove assignments from data set in solr", ex);
        }
    }

}
