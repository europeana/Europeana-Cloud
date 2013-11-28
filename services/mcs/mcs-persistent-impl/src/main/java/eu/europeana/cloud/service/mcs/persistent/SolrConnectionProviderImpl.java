
package eu.europeana.cloud.service.mcs.persistent;

import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.springframework.stereotype.Service;


/**
 * Establishes connection to Solr server.
 */
@Service
public class SolrConnectionProviderImpl implements SolrConnectionProvider {

    /**
     * Instance used for connecting with Solr server.
     */
    private SolrServer solrServer;

    /**
     * Class constructor. Expects Solr server URL.
     * 
     * @param solrUrl
     *            Solr server URL
     */
    public SolrConnectionProviderImpl(String solrUrl) {
        this.solrServer = new HttpSolrServer(solrUrl);
    }

    
    /**
     * Return solr server instance.
     * 
     * @return instance of Solr server
     */
    @Override
    public SolrServer getSolrServer() {
        return solrServer;
    }


    /**
     * Disconnects from Solr server.
     */
    public void disconnect() {
        solrServer.shutdown();
        solrServer = null;
    }
}
