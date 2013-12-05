package eu.europeana.cloud.service.mcs.persistent;

import java.io.File;

import javax.annotation.PreDestroy;

import org.apache.commons.io.FileUtils;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.core.CoreContainer;
import org.springframework.stereotype.Service;

/**
 * Establishes connection to embedded Solr.
 */
@Service
public class EmbeddedSolrConnectionProvider implements SolrConnectionProvider {

    /**
     * Instance used for connecting with Solr server.
     */
    private SolrServer solrServer;


    public EmbeddedSolrConnectionProvider() {
        File solrConfig = FileUtils.toFile(this.getClass().getResource("/solr_home/solr.xml"));
        File solrHome = FileUtils.toFile(this.getClass().getResource("/solr_home/"));

        CoreContainer container = CoreContainer.createAndLoad(solrHome.getAbsolutePath(), solrConfig);
        solrServer = new EmbeddedSolrServer(container, "");
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
    @PreDestroy
    public void disconnect() {
        solrServer.shutdown();
        solrServer = null;
    }
}
