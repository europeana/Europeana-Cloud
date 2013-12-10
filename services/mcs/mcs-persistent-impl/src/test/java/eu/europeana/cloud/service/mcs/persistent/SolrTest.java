package eu.europeana.cloud.service.mcs.persistent;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.io.File;
import java.io.IOException;
import java.util.Date;

import org.apache.commons.io.FileUtils;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.core.CoreContainer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class SolrTest {

    private SolrServer server;


    @Before
    public void setUp()
            throws Exception {
        File solrConfig = FileUtils.toFile(this.getClass().getResource("/solr_home/solr.xml"));
        File solrHome = FileUtils.toFile(this.getClass().getResource("/solr_home/"));

        CoreContainer container = CoreContainer.createAndLoad(solrHome.getAbsolutePath(), solrConfig);
        server = new EmbeddedSolrServer(container, "");

    }


    @Test
    public void test()
            throws Exception {
        RepresentationSolrDocument doc = new RepresentationSolrDocument("cid", "version", "dc", "provider", new Date(),
                true);

        server.addBean(doc);
        server.commit();
        SolrQuery q = new SolrQuery("version_id:version");
        RepresentationSolrDocument docFetched = server.query(q).getBeans(RepresentationSolrDocument.class).get(0);
        assertThat(docFetched, is(doc));
    }


    @After
    public void cleanUp()
            throws SolrServerException, IOException {
        server.deleteByQuery("*:*");
        server.commit();
        server.shutdown();
    }

}
