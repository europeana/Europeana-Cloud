package eu.europeana.cloud.service.mcs.persistent;

import eu.europeana.cloud.common.model.Representation;
import java.util.ArrayList;
import java.util.Date;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import static org.junit.Assert.*;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(value = { "classpath:/solrTestContext.xml" })
public class SolrDAOTest {

    @Autowired
    SolrDAO solrDAO;

    @Test
    public void shouldInsertRepresentation() throws Exception {
        Representation rep = new Representation("cloud1","schema1", "version1",null,null, "dataProvider", null, true, new Date());
        ArrayList<String> dataSets = new ArrayList();
        dataSets.add("dataSet1");
        dataSets.add("dataSet2");
        solrDAO.insertRepresentation(rep, dataSets);
        RepresentationSolrDocument doc = solrDAO.getDocumentById(rep.getVersion());
        assertEquals(rep.getVersion(), doc.getVersion());
        assertEquals(rep.getSchema(), doc.getSchema());
        assertEquals(rep.getDataProvider(), doc.getProviderId());
        assertTrue(doc.getDataSets().containsAll(dataSets));
    }
   

    @Test(expected = SolrException.class)
    public void shouldThrowExceptionWhenRequiredFieldMissing() throws Exception {
        Representation rep = new Representation(null,"schema","version",null,null,"dataProvider",null,true, new Date());
        solrDAO.insertRepresentation(rep, null);
    }
    
}
