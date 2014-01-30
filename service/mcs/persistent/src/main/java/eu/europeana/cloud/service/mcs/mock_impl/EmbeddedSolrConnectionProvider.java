package eu.europeana.cloud.service.mcs.mock_impl;

import java.io.File;
import java.net.URL;

import javax.annotation.PreDestroy;

import org.apache.commons.io.FileUtils;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.core.CoreContainer;
import org.springframework.stereotype.Service;

import eu.europeana.cloud.service.mcs.persistent.solr.SolrConnectionProvider;

/**
 * Establishes connection to embedded Solr.
 */
@Service
public class EmbeddedSolrConnectionProvider implements SolrConnectionProvider {

    /**
     * Instance used for connecting with Solr server.
     */
    private SolrServer solrServer;


    /**
     * Creates EmbeddedSolrServer instance using config files from solr_home directory.
     */
    public EmbeddedSolrConnectionProvider() {
        File solrConfig = findFile("/solr_home/solr.xml");
        File solrHome = findFile("/solr_home/");

        CoreContainer container = CoreContainer.createAndLoad(solrHome.getAbsolutePath(), solrConfig);
        solrServer = new EmbeddedSolrServer(container, "");
    }


    private File findFile(String filePath) {
        URL resource = this.getClass().getResource(filePath);
        if (resource == null) {
            throw new IllegalStateException("File " + filePath + " not found!");
        }
        File file = FileUtils.toFile(resource);
        if (file == null) {
            throw new IllegalStateException("File " + resource + " not found!");
        }
        if (!file.exists()) {
            throw new IllegalStateException("File " + file + " does not exist!");
        }
        return file;
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
