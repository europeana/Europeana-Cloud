package eu.europeana.cloud.service.mcs.persistent;

import com.google.common.base.Joiner;
import eu.europeana.cloud.common.model.File;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.service.mcs.RepresentationSearchParams;
import eu.europeana.cloud.service.mcs.persistent.exception.SolrDocumentNotFoundException;
import eu.europeana.cloud.service.mcs.persistent.exception.SystemException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import javax.annotation.PostConstruct;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Provides DAO operations for Solr.
 */
public class SolrDAO {

    @Autowired
    private SolrConnectionProvider connector;

    private SolrServer server;


    @PostConstruct
    public void init() {
        this.server = connector.getSolrServer();
    }


    /**
     * Removes dataset assigments from the previous representation version. Then inserts a new representation or updates
     * a previously added.
     * 
     * @param prevVersion
     *            name of the previous version of representation
     * @param rep
     *            representation to be inserted/updated
     * @param dataSetIds
     *            list of dataset ids
     * @throws IOException
     *             if there is a I/O error in Solr server
     * @throws SolrServerException
     *             if Solr server error occured
     * @throws SolrDocumentNotFoundException
     *             if document can't be found in Solr index
     */
    public void insertRepresentation(String prevVersion, Representation rep, Collection<CompoundDataSetId> dataSetIds)
            throws IOException, SolrServerException, SolrDocumentNotFoundException {
        // remove data set assignments from previous version of representation
        RepresentationSolrDocument prevRepr = getDocumentById(prevVersion);
        for (CompoundDataSetId compoundDataSetId : dataSetIds) {
            prevRepr.getDataSets().remove(serialize(compoundDataSetId));
        }
        server.addBean(prevRepr);

        // now, insert (or update) representation and add new data set assignments
        insertRepresentation(rep, dataSetIds);
    }


    /**
     * Inserts a new representation or updates a previously added (update might mean only setting persistent to true).
     * Sometimes such insertion involves dataset assignment changes - to indicate the most recent version in data set.
     * If this is the case, the list of data sets to which the representation should be assigned to is provided into
     * this method (if this is update, previously assigned data sets will remain). Moreover, those datasets should be
     * removed from previous persistent version of this representation.
     * 
     * @param rep
     * @param dataSetIds
     *            list of dataset ids
     * @throws IOException
     *             if there is a I/O error in Solr server
     * @throws SolrServerException
     *             if Solr server error occured
     */
    public void insertRepresentation(Representation rep, Collection<CompoundDataSetId> dataSetIds)
            throws IOException, SolrServerException {
        Collection<String> existingDataSets = new HashSet<>();
        try {
            existingDataSets.addAll(getDocumentById(rep.getVersion()).getDataSets());
        } catch (SolrDocumentNotFoundException ex) {
            //document does not exist - so insert it
        }

        if (dataSetIds != null) {
            for (CompoundDataSetId compoundDataSetId : dataSetIds) {
                existingDataSets.add(serialize(compoundDataSetId));
            }
        }

        RepresentationSolrDocument document = new RepresentationSolrDocument(rep.getRecordId(), rep.getVersion(),
                rep.getSchema(), rep.getDataProvider(), rep.getCreationDate(), rep.isPersistent(), existingDataSets);
        server.addBean(document);
        server.commit();
    }


    /**
     * Return document with given version_id from Solr.
     * 
     * @param versionId
     * @return retrieved document
     * @throws org.apache.solr.client.solrj.SolrServerException
     *             if Solr server error occured
     * @throws SolrDocumentNotFoundException
     *             if document can't be found in Solr index
     */
    public RepresentationSolrDocument getDocumentById(String versionId)
            throws SolrServerException, SolrDocumentNotFoundException {
        SolrQuery q = new SolrQuery(SolrFields.version + ":" + versionId);
        List<RepresentationSolrDocument> result = server.query(q).getBeans(RepresentationSolrDocument.class);
        if (result.isEmpty()) {
            throw new SolrDocumentNotFoundException(q.toString());
        }
        return result.get(0);
    }


    /**
     * Assigns given representation version to data set by adding data set identifier to representation document in
     * Solr.
     * 
     * @param versionId
     *            version of the representation
     * @param dataSetId
     *            dataset
     * @throws IOException
     *             if there is a I/O error in Solr server
     * @throws SolrServerException
     *             if Solr server error occured
     * @throws SolrDocumentNotFoundException
     *             if document can't be found in Solr index
     */
    public void addAssignment(String versionId, CompoundDataSetId dataSetId)
            throws SolrServerException, IOException, SolrDocumentNotFoundException {
        RepresentationSolrDocument document = getDocumentById(versionId);
        document.getDataSets().add(serialize(dataSetId));
        server.addBean(document);
        server.commit();
    }


    /**
     * Removes representation version from index.
     * 
     * @param versionId
     *            version of the representation
     * @throws IOException
     *             if there is a I/O error in Solr server
     * @throws SolrServerException
     *             if Solr server error occured
     */
    public void removeRepresentation(String versionId)
            throws SolrServerException, IOException {
        server.deleteById(versionId);
        server.commit();
    }


    /**
     * Removes representation with all history (all versions)
     * 
     * @param cloudId
     *            cloud identifier of the record
     * @param schema
     *            representation schema
     * @throws SolrServerException
     *             if Solr server error occured
     * @throws IOException
     *             if there is a I/O error in Solr server
     */
    public void removeRepresentation(String cloudId, String schema)
            throws SolrServerException, IOException {
        server.deleteByQuery(SolrFields.cloudId + ":" + cloudId + " AND " + SolrFields.schema + ":" + schema);
        server.commit();
    }


    /**
     * Removes data set from representation.
     * 
     * @param versionId
     *            version of the representation
     * @param dataSetId
     *            data set identifier
     * @throws IOException
     *             * if there is a I/O error in Solr server
     * @throws SolrServerException
     *             if Solr server error occured
     * @throws SolrDocumentNotFoundException
     *             if document can't be found in Solr index
     */
    public void removeAssignment(String versionId, CompoundDataSetId dataSetId)
            throws SolrServerException, IOException, SolrDocumentNotFoundException {
        RepresentationSolrDocument document = getDocumentById(versionId);
        document.getDataSets().remove(serialize(dataSetId));
        server.addBean(document);
        server.commit();
    }


    /**
     * Searches for representations matching given parameters. Generates Solr query from passed parameters and runs it
     * on Solr server. Number of results can be limited. Offset can be specified.
     * 
     * @param searchParams
     *            query parameters
     * @param startIndex
     *            indicates the offset in the complete result set
     * @param limit
     *            maximum number of results to return
     * @return list of representations matching parameters
     */
    public List<Representation> search(RepresentationSearchParams searchParams, int startIndex, int limit) {
        String queryString = generateQueryString(searchParams);
        SolrQuery query = new SolrQuery(queryString);
        query.setRows(limit).setStart(startIndex);
        try {
            QueryResponse response = server.query(query);
            List<RepresentationSolrDocument> foundDocuments = response.getBeans(RepresentationSolrDocument.class);
            List<Representation> representations = new ArrayList<>(foundDocuments.size());
            for (RepresentationSolrDocument document : foundDocuments) {
                representations.add(map(document));
            }
            return representations;
        } catch (SolrServerException ex) {
            throw new SystemException(ex);
        }

    }


    private String generateQueryString(RepresentationSearchParams params) {
        List<Param> queryParams = new ArrayList<>();
        if (params.getSchema() != null) {
            queryParams.add(new Param(SolrFields.schema, params.getSchema()));
        }
        if (params.getDataProvider() != null) {
            queryParams.add(new Param(SolrFields.providerId, params.getDataProvider()));
        }
        if (params.getDataSetId() != null) {
            queryParams.add(new Param(SolrFields.dataSets, params.getDataSetId()));
        }
        if (params.isPersistent() != null) {
            queryParams.add(new Param(SolrFields.persistent, params.isPersistent().toString()));
        }

        boolean anyDateIsSpecified = params.getFromDate() != null || params.getToDate() != null;
        if (anyDateIsSpecified) {
            DateTimeFormatter fmt = ISODateTimeFormat.dateTime();

            String fromDate;
            if (params.getFromDate() == null) {
                fromDate = "*";
            } else {
                fromDate = new DateTime(params.getFromDate()).withZone(DateTimeZone.UTC).toString(fmt);
            }
            String toDate;
            if (params.getToDate() == null) {
                toDate = "*";
            } else {
                toDate = new DateTime(params.getToDate()).withZone(DateTimeZone.UTC).toString(fmt);
            }
            String dateRangeParam = String.format("[%s TO %s]", fromDate, toDate);
            queryParams.add(new Param(SolrFields.creationDate, dateRangeParam));
        }
        return Joiner.on(" AND ").join(queryParams);
    }


    private Representation map(RepresentationSolrDocument document) {
        return new Representation(document.getCloudId(), document.getSchema(), document.getVersion(), null, null,
                document.getProviderId(), new ArrayList<File>(), document.isPersistent(), document.getCreationDate());
    }


    protected CompoundDataSetId deserialize(String serializedValue) {
        String[] values = serializedValue.split("\n");
        if (values.length != 2) {
            throw new IllegalArgumentException("Cannot construct proper compound data set id from value: "
                    + serializedValue);
        }
        return new CompoundDataSetId(values[0], values[1]);
    }


    protected String serialize(CompoundDataSetId dataSetId) {
        return dataSetId.getDataSetProviderId() + "\n" + dataSetId.getDataSetId();
    }


    private static final class Param {

        private final String field, value;


        @Override
        public String toString() {
            return field + ":(" + value + ")";
        }


        Param(String field, String value) {
            this.field = field;
            this.value = value;
        }
    }
}
