package eu.europeana.cloud.service.mcs.persistent.solr;

import javax.annotation.PreDestroy;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.springframework.stereotype.Component;

/**
 * Establishes connection to the Solr server.
 */
@Component
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
     * {@inheritDoc}
     */
    @Override
    public SolrServer getSolrServer() {
        return solrServer;
    }


    /**
     * Disconnects from Solr server.
     */
    @PreDestroy
    private void disconnect() {
        solrServer.shutdown();
        solrServer = null;
    }
}
