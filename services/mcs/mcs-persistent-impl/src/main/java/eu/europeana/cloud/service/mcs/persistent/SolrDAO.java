package eu.europeana.cloud.service.mcs.persistent;

import eu.europeana.cloud.common.model.DataSet;
import eu.europeana.cloud.common.model.Representation;
import java.util.Collection;
import java.util.Date;
import org.apache.solr.client.solrj.request.UpdateRequest;

/**
 *
 * @author sielski
 */
public abstract class SolrDAO {

	/**
	 * Inserts a new representation or updates a previously added (update might mean only setting persistent to true). 
	 * Sometimes such insertion involves dataset assignment changes - to indicate the most recent version in data set.
	 * If this is the case, the list of data sets to which the representation should be assigned to is provided into 
	 * this method (if this is update, previously assigned data sets will remain).
	 * Moreover, those datasets should be removed from previous persistent version of this representation.
	 *
	 * @param rep
	 * @param dataSetIds list of dataset ids
	 */
	public abstract void insertRepresentation(Representation rep, Collection<String> dataSetIds);


	/**
	 * Adds data set to representation.
	 *
	 * @param rep
	 * @param ds
	 */
	public abstract void addAssignment(Representation rep, DataSet ds);


	/**
	 * Removes representation.
	 *
	 * @param rep
	 */
	public abstract void removeRepresentation(Representation rep);


	/**
	 * Removes data set from representation.
	 *
	 * @param rep
	 * @param ds
	 */
	public abstract void removeAssignment(Representation rep, DataSet ds);
//	
//	public List<Representation> search(String schema, String dataProvider, boolean persistent, String dataSetId, Date fromDate, Date toDate, int startIndex, int limit)
}
