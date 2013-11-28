package eu.europeana.cloud.service.mcs.persistent;

import com.google.common.base.Joiner;
import eu.europeana.cloud.common.model.File;
import eu.europeana.cloud.common.model.Representation;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.PostConstruct;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.springframework.beans.factory.annotation.Autowired;

/**
 *
 * @author sielski
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
	 * @param prevVersion name of the previous version of representation
	 * @param rep representation to be inserted/updated
	 * @param dataSetIds list of dataset ids
	 * @throws IOException
	 * @throws SolrServerException
	 */
	public void insertRepresentation(String prevVersion, Representation rep, Collection<String> dataSetIds)
			throws IOException, SolrServerException {
		RepresentationSolrDocument prevRepr = getDocumentById(prevVersion);
		prevRepr.getDataSets().removeAll(dataSetIds);
		server.addBean(prevRepr);
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
	 * @param dataSetIds list of dataset ids
	 * @throws java.io.IOException
	 * @throws org.apache.solr.client.solrj.SolrServerException
	 */
	public void insertRepresentation(Representation rep, Collection<String> dataSetIds)
			throws IOException, SolrServerException {
		RepresentationSolrDocument document = new RepresentationSolrDocument(rep.getRecordId(),
				rep.getVersion(), rep.getSchema(), rep.getDataProvider(),
				new Date(), rep.isPersistent(), dataSetIds);

//            TODO - get the date from representation
		server.addBean(document);
		server.commit();
	}


	public RepresentationSolrDocument getDocumentById(String id)
			throws SolrServerException {
		SolrQuery q = new SolrQuery(SolrFields.version + ":" + id);
		return server.query(q).getBeans(RepresentationSolrDocument.class).get(0);
	}


	/**
	 * Adds data set to representation.
	 *
	 * @param versionId
	 * @param dataSetId
	 */
	public void addAssignment(String versionId, String dataSetId) {
	}


	/**
	 * Removes representation.
	 *
	 * @param versionId
	 * @throws org.apache.solr.client.solrj.SolrServerException
	 * @throws java.io.IOException
	 */
	public void removeRepresentation(String versionId)
			throws SolrServerException, IOException {
		server.deleteById(versionId);
		server.commit();
	}


	/**
	 * Removes data set from representation.
	 *
	 * @param versionId
	 * @param dataSetId
	 */
	public void removeAssignment(String versionId, String dataSetId) {
	}


	public List<Representation> search(String schema, String dataProvider, Boolean persistent, String dataSetId, Date fromDate, Date toDate, int startIndex, int limit) {
		
		List<Param> queryParams = new ArrayList<>();
		if (schema != null) {
			queryParams.add(new Param(SolrFields.schema, schema));
		}
		if (dataProvider != null) {
			queryParams.add(new Param(SolrFields.providerId, dataProvider));
		}
		if (dataSetId != null) {
			queryParams.add(new Param(SolrFields.dataSets, dataSetId));
		}
		if (persistent != null) {
			queryParams.add(new Param(SolrFields.persistent, persistent.toString()));
		}
		String dateRangeParam;
		if (fromDate != null) {
			dateRangeParam = fromDate.toString();
		} else {
			dateRangeParam = "*";
		}
		dateRangeParam += " TO ";
		if (toDate != null) {
			dateRangeParam += toDate.toString();
		} else {
			dateRangeParam += "*";
		}
		if (dateRangeParam.equals("* TO *")) {
			queryParams.add(new Param(SolrFields.creationDate, dateRangeParam));
		}
		String queryString = Joiner.on(" AND ").join(queryParams);
		SolrQuery query = new SolrQuery(queryString);
		query.setRows(limit).setStart(startIndex);
		try {
			QueryResponse response = server.query(query);
			List<RepresentationSolrDocument> foundDocuments = response.getBeans(RepresentationSolrDocument.class);
			List<Representation> representations = new ArrayList<>(foundDocuments.size());
			for (RepresentationSolrDocument document: foundDocuments) {
				representations.add(map(document));
			}
			return representations;
		} catch (SolrServerException ex) {
			throw new SystemException(ex);
		}

	}
	
	private Representation map(RepresentationSolrDocument document) {
		return new Representation(document.getCloudId(), document.getSchema(), document.getVersion(), null, null,
				document.getProviderId(), new ArrayList<File>(), document.isPersistent(), document.getCreationDate());
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
