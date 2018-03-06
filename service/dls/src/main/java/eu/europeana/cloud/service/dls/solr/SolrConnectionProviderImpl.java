package eu.europeana.cloud.service.dls.solr;

import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.springframework.stereotype.Component;

import javax.annotation.PreDestroy;

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
     * Class constructor. Expects SolrServer object.
     * 
     * @param solrServer
     *            Solr solrServer object
     */
    public SolrConnectionProviderImpl(SolrServer solrServer) {
        this.solrServer = solrServer;
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
