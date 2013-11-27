package eu.europeana.cloud.service.mcs.persistent;

import java.io.File;
import org.apache.commons.io.FileUtils;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.core.CoreContainer;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(value = { "classpath:/solrTestContext.xml" })
public class SolrTest {

    private SolrServer server;
    
    @Before
    public void setUp() {
        File solrConfig = FileUtils.toFile(this.getClass().getResource("/solr_home/solr.xml"));
        File solrHome  = FileUtils.toFile(this.getClass().getResource("/solr_home/"));
       
        CoreContainer container = CoreContainer.createAndLoad(solrHome.getAbsolutePath(), solrConfig);
        server = new EmbeddedSolrServer(container, "");
    }
    
    @Test
    public void test(){
        
    }
    
    
    public void cleanUp(){
        server.shutdown();
    }
    
}
