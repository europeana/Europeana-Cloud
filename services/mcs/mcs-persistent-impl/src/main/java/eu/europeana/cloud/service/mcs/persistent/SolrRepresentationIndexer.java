package eu.europeana.cloud.service.mcs.persistent;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

import org.apache.solr.client.solrj.SolrServerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.service.mcs.persistent.exception.SolrDocumentNotFoundException;

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

    private final static Logger log = LoggerFactory.getLogger(SolrRepresentationIndexer.class);

    @Autowired
    private SolrDAO solrDAO;

    @Autowired
    private CassandraDataSetDAO cassandraDataSetDAO;


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
            log.error("Cannot insert representation into solr", ex);
        }
    }


    public void removeRepresentationVersion(String versionId) {
        try {
            solrDAO.removeRepresentationVersion(versionId);
        } catch (SolrServerException | IOException ex) {
            log.error("Cannot remove representation from solr", ex);
        }
    }


    public void removeRepresentation(String cloudId, String schema) {
        try {
            solrDAO.removeRepresentation(cloudId, schema);
        } catch (SolrServerException | IOException ex) {
            log.error("Cannot remove representation from solr", ex);
        }
    }


    public void removeRecordRepresentations(String cloudId) {
        try {
            solrDAO.removeRecordRepresentation(cloudId);
        } catch (SolrServerException | IOException ex) {
            log.error("Cannot remove representation from solr", ex);
        }
    }


    public void removeAssignment(String recordId, String schema, CompoundDataSetId dataSetId) {
        try {
            solrDAO.removeAssignment(recordId, schema, Collections.singletonList(dataSetId));
        } catch (SolrServerException | IOException | SolrDocumentNotFoundException ex) {
            log.error("Cannot remove assignment from solr", ex);
        }
    }


    public void addAssignment(String versionId, CompoundDataSetId dataSetId) {
        try {
            solrDAO.addAssignment(versionId, dataSetId);
        } catch (SolrServerException | IOException | SolrDocumentNotFoundException ex) {
            log.error("Cannot add assignment to solr", ex);
        }
    }


    public void removeAssignmentsFromDataSet(CompoundDataSetId dataSetId) {
        try {
            solrDAO.removeAssignmentFromDataSet(dataSetId);
        } catch (SolrServerException | IOException ex) {
            log.error("Cannot remove assignments from data set in solr", ex);
        }
    }

}
