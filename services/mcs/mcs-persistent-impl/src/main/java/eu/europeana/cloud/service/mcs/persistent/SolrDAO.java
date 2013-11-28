package eu.europeana.cloud.service.mcs.persistent;

import eu.europeana.cloud.common.model.Representation;
import java.io.IOException;
import java.util.Collection;
import java.util.Date;
import javax.annotation.PostConstruct;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
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
        public void init(){
                this.server = connector.getSolrServer();
        }
        
        /**
         * Removes dataset assigments from the previous representation version. 
         * Then inserts a new representation or updates a previously added.
         * 
         * @param prevVersion name of the previous version of representation
         * @param rep representation to be inserted/updated
         * @param dataSetIds list of dataset ids
         * @throws IOException
         * @throws SolrServerException 
         */
        public void insertRepresentation(String prevVersion, Representation rep, Collection<String> dataSetIds) throws IOException, SolrServerException {
            RepresentationSolrDocument prevRepr = getDocumentById(prevVersion);
            prevRepr.getDataSets().removeAll(dataSetIds);
            server.addBean(prevRepr);
            insertRepresentation(rep,dataSetIds);
        }
        
	/**
	 * Inserts a new representation or updates a previously added (update might mean only setting persistent to true). 
	 * Sometimes such insertion involves dataset assignment changes - to indicate the most recent version in data set.
	 * If this is the case, the list of data sets to which the representation should be assigned to is provided into 
	 * this method (if this is update, previously assigned data sets will remain).
	 * Moreover, those datasets should be removed from previous persistent version of this representation.
	 *
	 * @param rep
	 * @param dataSetIds list of dataset ids
         * @throws java.io.IOException
         * @throws org.apache.solr.client.solrj.SolrServerException
	 */
	public void insertRepresentation(Representation rep, Collection<String> dataSetIds) throws IOException, SolrServerException {
            RepresentationSolrDocument document = new RepresentationSolrDocument(rep.getRecordId(),
                    rep.getVersion(), rep.getSchema(), rep.getDataProvider(), 
                    new Date(), rep.isPersistent(), dataSetIds);

//            TODO - get the date from representation
            server.addBean(document);
            server.commit();
        }
        
        public RepresentationSolrDocument getDocumentById(String id) throws SolrServerException {
            SolrQuery q = new SolrQuery(SolrFields.version +":" +id);
            return server.query(q).getBeans(RepresentationSolrDocument.class).get(0);
        }
        
        
	/**
	 * Adds data set to representation.
	 *
         * @param versionId
         * @param dataSetId
	 */
	public void addAssignment(String versionId, String dataSetId){
        }


	/**
	 * Removes representation.
	 *
	 * @param versionId
         * @throws org.apache.solr.client.solrj.SolrServerException
         * @throws java.io.IOException
	 */
	public void removeRepresentation(String versionId) throws SolrServerException, IOException{
            server.deleteById(versionId);
            server.commit();
        }


	/**
	 * Removes data set from representation.
	 *
	 * @param versionId
	 * @param dataSetId
	 */
	public void removeAssignment(String versionId, String dataSetId){}
}
