package eu.europeana.cloud.service.dls.solr;

import eu.europeana.cloud.service.dls.solr.exception.SystemException;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.Collections2;
import eu.europeana.cloud.common.model.CompoundDataSetId;
import eu.europeana.cloud.common.model.File;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.service.dls.RepresentationSearchParams;
import eu.europeana.cloud.service.dls.solr.exception.SolrDocumentNotFoundException;
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
import org.apache.solr.client.solrj.util.ClientUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

/**
 * Provides DAO operations for Solr.
 */
@Repository
public class SolrDAO {

    @Autowired
    private SolrConnectionProvider connector;

    private SolrServer server;

    // separator between provider id and dataset id in serialized compund dataset id
    protected static final String CDSID_SEPARATOR = "\n";


    /**
     * Initialize Solr connection after bean is constructed.
     */
    @PostConstruct
    public void init() {
        this.server = connector.getSolrServer();
    }


    /**
     * Inserts or updates a representation (update might mean only setting persistent to true and changing date). If
     * representation is inserted, it will be assigned to provided list of data sets. If representation is updated, its
     * prevoiusly assigned data set will remain and provided list of data sets will be also added.
     * 
     * @param rep
     *            representation to be added or updated
     * @param dataSetIds
     *            list of dataset ids
     * @throws java.io.IOException
     *             if there is a I/O error in Solr server
     * @throws org.apache.solr.client.solrj.SolrServerException
     *             if Solr server error occurred
     */
    public void insertRepresentation(Representation rep, Collection<CompoundDataSetId> dataSetIds)
            throws IOException, SolrServerException {
        // collect list of previously assigned data sets
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

        RepresentationSolrDocument document = new RepresentationSolrDocument(rep.getCloudId(), rep.getVersion(),
                rep.getRepresentationName(), rep.getDataProvider(), rep.getCreationDate(), rep.isPersistent(),
                existingDataSets);
        server.addBean(document);
        server.commit();
    }


    /**
     * Return document with given version_id from Solr.
     * 
     * @param versionId
     * @return retrieved document
     * @throws org.apache.solr.client.solrj.SolrServerException
     *             if Solr server error occurred
     * @throws SolrDocumentNotFoundException
     *             if document can't be found in Solr index
     */
    public RepresentationSolrDocument getDocumentById(String versionId)
            throws SolrServerException, SolrDocumentNotFoundException {
        SolrQuery q = new SolrQuery(SolrFields.VERSION + ":" + versionId);
        List<RepresentationSolrDocument> result = server.query(q).getBeans(RepresentationSolrDocument.class);
        if (result.isEmpty()) {
            throw new SolrDocumentNotFoundException(q.toString());
        }
        return result.get(0);
    }


    /**
     * Adds data set to representation.
     * 
     * @param versionId
     *            representation version id
     * @param dataSetId
     *            compound data set id
     * @throws java.io.IOException
     *             if there is a I/O error in Solr server
     * @throws org.apache.solr.client.solrj.SolrServerException
     *             if Solr server error occurred
     * @throws SolrDocumentNotFoundException
     *             if document can't be found in Solr index
     */
    public void addAssignment(String versionId, CompoundDataSetId dataSetId)
            throws SolrServerException, IOException, SolrDocumentNotFoundException {
        RepresentationSolrDocument document = getDocumentById(versionId);
        Collection<String> dataSets = document.getDataSets();
        String assigment = serialize(dataSetId);
        if (!dataSets.contains(assigment)) {
            dataSets.add(assigment);
        }

        server.addBean(document);
        server.commit();
    }


    /**
     * Removes representation version.
     * 
     * @param versionId
     * @throws java.io.IOException
     *             if there is a I/O error in Solr server
     * @throws org.apache.solr.client.solrj.SolrServerException
     *             if Solr server error occurred
     */
    public void removeRepresentationVersion(String versionId)
            throws SolrServerException, IOException {
        server.deleteById(versionId);
        server.commit();
    }


    /**
     * Removes representation with all history (all versions)
     * 
     * @param cloudId
     * @param schema
     * @throws SolrServerException
     *             if Solr server error occurred
     * @throws IOException
     *             if there is a I/O error in Solr server
     */
    public void removeRepresentation(String cloudId, String schema)
            throws SolrServerException, IOException {
        server.deleteByQuery(SolrFields.CLOUD_ID + ":" + cloudId + " AND " + SolrFields.SCHEMA + ":" + schema);
        server.commit();
    }


    /**
     * Removes all record representations with all history (all versions).
     * 
     * @param cloudId
     * @throws SolrServerException
     * @throws IOException
     */
    public void removeRecordRepresentation(String cloudId)
            throws SolrServerException, IOException {
        server.deleteByQuery(SolrFields.CLOUD_ID + ":" + cloudId);
        server.commit();
    }


    /**
     * Removes data set from representation.
     * 
     * @param recordId
     *            record id
     * @param schema
     *            representation schema
     * @param dataSetIds
     *            list of data sets to be removed from representation
     * @throws java.io.IOException
     *             if there is a I/O error in Solr server
     * @throws org.apache.solr.client.solrj.SolrServerException
     *             if Solr server error occurred
     * @throws SolrDocumentNotFoundException
     *             if document can't be found in Solr index
     */
    public void removeAssignment(String recordId, String schema, Collection<CompoundDataSetId> dataSetIds)
            throws SolrServerException, IOException, SolrDocumentNotFoundException {
        RepresentationSearchParams params = RepresentationSearchParams.builder().setRecordId(recordId)
                .setSchema(schema).build();
        String queryString = generateQueryString(params);
        SolrQuery query = new SolrQuery(queryString);
        QueryResponse response = server.query(query);
        Collection<String> serializedDataSetIds = Collections2.transform(dataSetIds,
            new Function<CompoundDataSetId, String>() {

                @Override
                public String apply(CompoundDataSetId input) {
                    return serialize(input);
                }
            });
        List<RepresentationSolrDocument> foundDocuments = response.getBeans(RepresentationSolrDocument.class);
        for (RepresentationSolrDocument document : foundDocuments) {
            if (document.getDataSets().removeAll(serializedDataSetIds)) {
                server.addBean(document);
            }
        }
        server.commit();
    }


    /**
     * Removes data set assignments from ALL representation. This method is to be used if whole data set is removed.
     * 
     * @param dataSetId
     *            data set id
     * @throws java.io.IOException
     *             if there is a I/O error in Solr server
     * @throws org.apache.solr.client.solrj.SolrServerException
     *             if Solr server error occurred
     */
    public void removeAssignmentFromDataSet(CompoundDataSetId dataSetId)
            throws SolrServerException, IOException {
        RepresentationSearchParams params = RepresentationSearchParams.builder()
                .setDataSetProviderId(dataSetId.getDataSetProviderId()).setDataSetId(dataSetId.getDataSetId()).build();
        String queryString = generateQueryString(params);
        SolrQuery query = new SolrQuery(queryString);
        QueryResponse response = server.query(query);
        String serializedDataSetId = serialize(dataSetId);
        List<RepresentationSolrDocument> foundDocuments = response.getBeans(RepresentationSolrDocument.class);
        for (RepresentationSolrDocument document : foundDocuments) {
            if (document.getDataSets().remove(serializedDataSetId)) {
                server.addBean(document);
            }
        }
        server.commit();
    }


    /**
     * Removes data set assignment from representation version.
     * 
     * @param versionId
     *            version of representation
     * @param dataSetId
     *            data set id
     * @throws java.io.IOException
     *             if there is a I/O error in Solr server
     * @throws org.apache.solr.client.solrj.SolrServerException
     *             if Solr server error occurred
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
     * Searches for representation versions that satisfy query parameters. Generates Solr query from passed parameters
     * and runs it on Solr server. Number of results can be limited. Offset can be specified.
     * 
     * @param searchParams
     *            parameters of representation versions to be found.
     * @param startIndex
     *            offset of returned list of representations.
     * @param limit
     *            maximum length of returned list.
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


    /**
     * Generates solr query string from parameters.
     * 
     * @param params
     *            query parameters.
     * @return
     */
    private String generateQueryString(RepresentationSearchParams params) {
        List<Param> queryParams = new ArrayList<>();
        if (params.getSchema() != null) {
            String encodedValue = ClientUtils.escapeQueryChars(params.getSchema());
            queryParams.add(new Param(SolrFields.SCHEMA, encodedValue));
        }
        if (params.getRecordId() != null) {
            String encodedValue = ClientUtils.escapeQueryChars(params.getRecordId());
            queryParams.add(new Param(SolrFields.CLOUD_ID, encodedValue));
        }
        if (params.getDataProvider() != null) {
            String encodedValue = ClientUtils.escapeQueryChars(params.getDataProvider());
            queryParams.add(new Param(SolrFields.PROVIDER_ID, encodedValue));
        }
        if (params.getDataSetId() != null && params.getDataSetProviderId() != null) {
            CompoundDataSetId compoundDataSetId = new CompoundDataSetId(params.getDataSetProviderId(),
                    params.getDataSetId());
            String encodedValue = ClientUtils.escapeQueryChars(serialize(compoundDataSetId));
            queryParams.add(new Param(SolrFields.DATA_SETS, encodedValue));
        } else if (params.getDataSetId() != null) {
            String encodedValue = ClientUtils.escapeQueryChars(CDSID_SEPARATOR + params.getDataSetId());
            queryParams.add(new Param(SolrFields.DATA_SETS, "*" + encodedValue));
        } else if (params.getDataSetProviderId() != null) {
            String encodedValue = ClientUtils.escapeQueryChars(params.getDataSetProviderId() + CDSID_SEPARATOR);
            queryParams.add(new Param(SolrFields.DATA_SETS, encodedValue + "*"));
        }
        if (params.isPersistent() != null) {
            queryParams.add(new Param(SolrFields.PERSISTENT, params.isPersistent().toString()));
        }

        // if start of end of creation date range is specified - add range parameter
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
            queryParams.add(new Param(SolrFields.CREATION_DATE, dateRangeParam));
        }
        return Joiner.on(" AND ").join(queryParams);
    }


    /**
     * Maps solr document bean into representation class object.
     * 
     * @param document
     * @return
     */
    private Representation map(RepresentationSolrDocument document) {
        return new Representation(document.getCloudId(), document.getSchema(), document.getVersion(), null, null,
                document.getProviderId(), new ArrayList<File>(), document.isPersistent(), document.getCreationDate());
    }


    protected CompoundDataSetId deserialize(String serializedValue) {
        String[] values = serializedValue.split(CDSID_SEPARATOR);
        if (values.length != 2) {
            throw new IllegalArgumentException("Cannot construct proper compound data set id from value: "
                    + serializedValue);
        }
        return new CompoundDataSetId(values[0], values[1]);
    }


    protected String serialize(CompoundDataSetId dataSetId) {
        return dataSetId.getDataSetProviderId() + CDSID_SEPARATOR + dataSetId.getDataSetId();
    }


    /**
     * Represents solr query parameter: field name and expected value.
     */
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
