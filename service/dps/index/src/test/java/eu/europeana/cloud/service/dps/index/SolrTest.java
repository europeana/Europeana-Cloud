package eu.europeana.cloud.service.dps.index;
/*
import eu.europeana.cloud.service.dps.index.exception.IndexerException;
import eu.europeana.cloud.service.dps.index.structure.IndexedDocument;
import eu.europeana.cloud.service.dps.index.structure.SearchResult;
import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.client.solrj.request.CoreAdminRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrInputDocument;
import org.junit.After;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.Test;
*/
/**
 *
 * @author Pavel Kefurt <Pavel.Kefurt@gmail.com>
 *//*
public class SolrTest
{
    private SolrClient client;

    @Before
    public void before() throws URISyntaxException, SolrServerException, IOException
    {
        client = new EmbeddedSolrServer(Paths.get(getClass().getResource("/solr_home").toURI()), "test_core"); 
        
        CoreAdminRequest.Create createRequest = new CoreAdminRequest.Create();
        createRequest.setCoreName("test_core");
        createRequest.setConfigSet("minimal");
        client.request(createRequest);
    }
    
    @After
    public void after() throws IOException
    {
        client.close();
        
        File f = FileUtils.toFile(getClass().getResource("/solr_home/test_core"));
        if(f != null && f.exists())
        {
            FileUtils.deleteDirectory(f);
        }
    }
    
    @Test
    public void searchTest() throws IndexerException, SolrServerException, IOException
    {
        SolrInputDocument document1 = new SolrInputDocument();
        document1.addField("id", "1");  
        document1.addField("field1", "value1"); 
        SolrInputDocument document2 = new SolrInputDocument();
        document2.addField("id", "2");   
        document2.addField("field1", "value2"); 
        SolrInputDocument document3 = new SolrInputDocument();
        document3.addField("id", "3");
        document3.addField(IndexFields.RAW_TEXT.toString(), "some full text with lot of informations");
        SolrInputDocument document4 = new SolrInputDocument();
        document4.addField("id", "4");
        document4.addField(IndexFields.RAW_TEXT.toString(), "dog is an animal");
        SolrInputDocument document5 = new SolrInputDocument();
        document5.addField("id", "5");
        document5.addField("field1", "value3");
        document5.addField("field2", "value1 and value2");
        SolrInputDocument document6 = new SolrInputDocument();
        document6.addField("id", "6");
        document6.addField("field1", "value4");
        document6.addField("field2", "value5");
        SolrInputDocument document7 = new SolrInputDocument();
        document7.addField("id", "7");
        document7.addField("field3", "my phrase");
        SolrInputDocument document8 = new SolrInputDocument();
        document8.addField("id", "8");
        document8.addField("field3", "my dog and phrase");
        SolrInputDocument document9 = new SolrInputDocument();
        document9.addField("id", "9");
        document9.addField("field3", "my dog and cat");
        SolrInputDocument document10 = new SolrInputDocument();
        document10.addField("id", "10");
        document10.addField("field3", "my dog and your cat");
        SolrInputDocument document11 = new SolrInputDocument();
        document11.addField("id", "11");
        document11.addField("field3", "my cat and dog");
        
        client.add(document1);
        client.add(document2);
        client.add(document3);
        client.add(document4);
        client.add(document5);
        client.add(document6);
        client.add(document7);
        client.add(document8);
        client.add(document9);
        client.add(document10);
        client.add(document11);
        
        client.commit();
        
        Solr es = new Solr(client, "test_core");
        
        SearchResult result;
        
        //----- search -----
        
        String[] fields1 = {"field1"};
        result = es.search("value2", fields1);       
        assertTrue(result.getTotalHits() == 1);
        
        String[] fields2 = {"field1", "field2"};
        result = es.search("value2", fields2); 
        assertTrue(result.getTotalHits() == 2);
        
        result = es.search("xxxxx", fields2); 
        assertTrue(result.getTotalHits() == 0);
        
        result = es.search("value2", fields2, 1, Indexer.TIMEOUT);       
        assertTrue(result.getTotalHits() == 2);
        assertTrue(result.getHits().size() == 1);
        
        result = es.search("value2", fields2, -2, Indexer.TIMEOUT);       
        assertTrue(result.getTotalHits() == 0);
        
        result = es.search("value2", null);       
        assertTrue(result.getTotalHits() == 0);
        
        result = es.search(null, fields1);       
        assertTrue(result.getTotalHits() == 0);
        
        //----- full text -----
        
        result = es.searchFullText("cat is animal");
        assertTrue(result.getTotalHits() == 1);
        
        result = es.searchFullText("superman and batman");
        assertTrue(result.getTotalHits() == 0);
        
        result = es.searchFullText(null);
        assertTrue(result.getTotalHits() == 0);
        
        //----- phrase -----
        
        result = es.searchPhrase("dog and cat", "field3", 0);
        assertTrue(result.getTotalHits() == 1);
        
        result = es.searchPhrase("dog and cat", "field3", 1);
        assertTrue(result.getTotalHits() == 2);
        
        result = es.searchPhrase("dog and cat", "field3", 4);
        assertTrue(result.getTotalHits() == 3);
        
        result = es.searchPhrase("xxx ccc", "field3", 1);
        assertTrue(result.getTotalHits() == 0);
        
        result = es.searchPhrase("dog and cat", "field3", -1);
        assertTrue(result.getTotalHits() == 0);
        
        result = es.searchPhrase("dog and cat", "field3", 1, -2, Indexer.TIMEOUT);
        assertTrue(result.getTotalHits() == 0);
        
        result = es.searchPhrase("dog and cat", null, 1);
        assertTrue(result.getTotalHits() == 0);
        
        result = es.searchPhrase(null, "field3", 1);
        assertTrue(result.getTotalHits() == 0);

        //----- advanced search -----
        
        result = es.advancedSearch("field3:(+dog -your phrase^4)");
        assertTrue(result.getTotalHits() == 3);
        assertEquals("8", result.getHits().get(0).getId());
        
        Map<String, Object> p1 = new HashMap<>();
        p1.put("default_operator", Indexer.Operator.AND);
        result = es.advancedSearch("field3:(+dog -your phrase^4)", p1);
        assertTrue(result.getTotalHits() == 1); 
        
        Map<String, Object> p2 = new HashMap<>();
        p2.put("default_field", "field3");
        result = es.advancedSearch("+dog -your phrase^4", p2);
        assertTrue(result.getTotalHits() == 3); 
  
        result = es.advancedSearch("field3:(+dog -your phrase^4)", null);
        assertTrue(result.getTotalHits() == 3);
        
        result = es.advancedSearch("field3:(+dog -your phrase^4)", null, -2, Indexer.TIMEOUT);
        assertTrue(result.getTotalHits() == 0);
        
        result = es.advancedSearch("");
        assertTrue(result.getTotalHits() == 0);
        
        result = es.advancedSearch(null);
        assertTrue(result.getTotalHits() == 0);
    }
    
    @Test
    public void moreLikeThisTest() throws IndexerException, IOException, SolrServerException
    {
        SolrInputDocument document7 = new SolrInputDocument();
        document7.addField("id", "7");
        document7.addField("field3", "my phrase");
        SolrInputDocument document8 = new SolrInputDocument();
        document8.addField("id", "8");
        document8.addField("field3", "my dog and phrase");
        SolrInputDocument document9 = new SolrInputDocument();
        document9.addField("id", "9");
        document9.addField("field3", "my dog and cat");
        SolrInputDocument document10 = new SolrInputDocument();
        document10.addField("id", "10");
        document10.addField("field3", "my dog and your cat");
        SolrInputDocument document11 = new SolrInputDocument();
        document11.addField("id", "11");
        document11.addField("field3", "my cat and dog");

        client.add(document7);
        client.add(document8);
        client.add(document9);
        client.add(document10);
        client.add(document11);
        
        client.commit();
        
        Solr es = new Solr(client, "test_core");
        
        SearchResult result;

        String[] fields3 = {"field3", IndexFields.RAW_TEXT.toString()};
        result = es.getMoreLikeThis("9",fields3, 20, 1, 1, 20, 1, 20, 10, 0, true);
        assertTrue(result.getTotalHits() == 5);
        
        result = es.getMoreLikeThis("9",fields3, 20, 1, 1, 20, 1, 20, 10, 0, false);
        assertTrue(result.getTotalHits() == 4);
        
        result = es.getMoreLikeThis("9", null, 20, 1, 1, 20, 1, 20, 10, 0, false);
        assertTrue(result.getTotalHits() == 4);
        
        result = es.getMoreLikeThis("9", null, 20, 1, 1, 20, 1, 20, -2, 0, false);
        assertTrue(result.getTotalHits() == 0);
        
        result = es.getMoreLikeThis("", null, 20, 1, 1, 20, 1, 20, 10, 0, false);
        assertTrue(result.getTotalHits() == 0);
        
        result = es.getMoreLikeThis(null, null, 20, 1, 1, 20, 1, 20, 10, 0, false);
        assertTrue(result.getTotalHits() == 0);
    }
    
    @Test
    public void scrollTest() throws IndexerException, SolrServerException, IOException
    {
        SolrInputDocument document1 = new SolrInputDocument();
        document1.addField("id", "1");  
        document1.addField("field1", "value1"); 
        SolrInputDocument document2 = new SolrInputDocument();
        document2.addField("id", "2");   
        document2.addField("field1", "value2"); 
        SolrInputDocument document5 = new SolrInputDocument();
        document5.addField("id", "5");
        document5.addField("field1", "value3");
        document5.addField("field2", "value1 and value2");
        SolrInputDocument document6 = new SolrInputDocument();
        document6.addField("id", "6");
        document6.addField("field1", "value4");
        document6.addField("field2", "value5");
        
        client.add(document1);
        client.add(document2);
        client.add(document5);
        client.add(document6);
        
        client.commit();
        
        Solr es = new Solr(client, "test_core");
        
        SearchResult result;
        
        String[] fields2 = {"field1", "field2"};
        result = es.search("value2", fields2, 1, 5000); 
        assertTrue(result.getTotalHits() == 2);
        assertTrue(result.getHits().size() == 1);
        
        result = es.getNextPage(result.getScrollId(), result);
        assertTrue(result.getTotalHits() == 2);
        assertTrue(result.getHits().size() == 1);
        
        result = es.getNextPage(result.getScrollId(), result);
        assertNull(result.getScrollId());        
    }
 
    @Test
    public void manipulationTest() throws IndexerException, SolrServerException, IOException
    {        
        SolrInputDocument document1 = new SolrInputDocument();
        document1.addField("id", "1");  
        document1.addField("field1", "value1"); 
        
        client.add(document1);
        
        client.commit();
        
        Solr es = new Solr(client, "test_core");
        
        //----
        Map<String, Object> data = new HashMap<>();
        data.put("field2", "value2");
        data.put("field1", "value99");
        es.insert(data);
        es.insert("keyX", data);
      
        assertTrue(client.query(new SolrQuery("*:*")).getResults().getNumFound() == 3);
        
        //----
        Map<String, Object> data2 = new HashMap<>();
        data.put("field1", "value9");
        es.update("keyX", data);
        
        QueryResponse response = client.query(new SolrQuery("*:*"));
        assertTrue(response.getResults().getNumFound() == 3);
        for(SolrDocument hit: response.getResults())
        {
            if("keyX".equals(hit.get("id")))
            {
                assertEquals("value9", ((Collection<Object>)hit.get("field1")).toArray()[0]);
                assertEquals("value2", ((Collection<Object>)hit.get("field2")).toArray()[0]);
                break;
            }
        }
        
        //----
        es.update("keyX", (String)null);
        
        response = client.query(new SolrQuery("*:*"));
        assertTrue(response.getResults().getNumFound() == 3);
        for(SolrDocument hit: response.getResults())
        {
            if("keyX".equals(hit.get("id")))
            {
                assertEquals("value9", ((Collection<Object>)hit.get("field1")).toArray()[0]);
                assertEquals("value2", ((Collection<Object>)hit.get("field2")).toArray()[0]);
                break;
            }
        }
        
        //----
        es.update("keyX", (Map)null);
        
        response = client.query(new SolrQuery("*:*"));
        assertTrue(response.getResults().getNumFound() == 3);
        for(SolrDocument hit: response.getResults())
        {
            if("keyX".equals(hit.get("id")))
            {
                assertEquals("value9", ((Collection<Object>)hit.get("field1")).toArray()[0]);
                assertEquals("value2", ((Collection<Object>)hit.get("field2")).toArray()[0]);
                break;
            }
        }
        
        //----
        es.update("keyX", new HashMap<String, Object>());
        
        response = client.query(new SolrQuery("*:*"));
        assertTrue(response.getResults().getNumFound() == 3);
        for(SolrDocument hit: response.getResults())
        {
            if("keyX".equals(hit.get("id")))
            {
                assertEquals("value9", ((Collection<Object>)hit.get("field1")).toArray()[0]);
                assertEquals("value2", ((Collection<Object>)hit.get("field2")).toArray()[0]);
                break;
            }
        }
        
        //----
        es.update("keyX", "");
        
        response = client.query(new SolrQuery("*:*"));
        assertTrue(response.getResults().getNumFound() == 3);
        for(SolrDocument hit: response.getResults())
        {
            if("keyX".equals(hit.get("id")))
            {
                assertEquals("value9", ((Collection<Object>)hit.get("field1")).toArray()[0]);
                assertEquals("value2", ((Collection<Object>)hit.get("field2")).toArray()[0]);
                break;
            }
        }
        
        //----
        es.delete("1");
        assertTrue(client.query(new SolrQuery("*:*")).getResults().getNumFound() == 2);
        
        //----
        es.delete(null);
        assertTrue(client.query(new SolrQuery("*:*")).getResults().getNumFound() == 2);
        
        //----
        es.delete("");
        assertTrue(client.query(new SolrQuery("*:*")).getResults().getNumFound() == 2);
        
        //----
        es.insert(null, data);      
        assertTrue(client.query(new SolrQuery("*:*")).getResults().getNumFound() == 3);
        
        //----
        es.insert("2", new HashMap<String, Object>());       
        assertTrue(client.query(new SolrQuery("*:*")).getResults().getNumFound() == 3);
        
        //----
        es.insert("3", "");      
        assertTrue(client.query(new SolrQuery("*:*")).getResults().getNumFound() == 3);
        
        //----
        es.insert("4", (String)null);      
        assertTrue(client.query(new SolrQuery("*:*")).getResults().getNumFound() == 3);
        
        //----
        es.insert("4", (Map)null);      
        assertTrue(client.query(new SolrQuery("*:*")).getResults().getNumFound() == 3);
        
        //---- get ----
        IndexedDocument document = es.getDocument("keyX");
        assertTrue(document.hasData());
        assertEquals("value9", document.getData().get("field1"));
        assertEquals("value2", document.getData().get("field2"));
        
        document = es.getDocument("xxxx");
        assertNull(document);
        
        document = es.getDocument("");
        assertNull(document);
        
        document = es.getDocument(null);
        assertNull(document);
    }
    
    @Test
    public void complexDataManipulationTest() throws IndexerException
    {
        String data1 = "{\"field1\":\"value1\", \"field2\":[\"F2_val1\", \"F2_val2\"],"
                + "\"field3\":{\"F3_F1\":\"F3_F1_val\", \"F3_F2\":[\"F3_F2_val1\", \"F3_F2_val2\"]},"
                + "\"field4\":[{\"F4_V_F1\":\"F4_V_F1_val\"}, {\"F4_V_F2\":\"F4_V_F2_val\"}]}";
        
        List<Object> forField2 = new ArrayList<>();
        forField2.add("F2_val1");
        forField2.add("F2_val2");
        
        List<Object> forFieldF3_F2 = new ArrayList<>();
        forFieldF3_F2.add("F3_F2_val1");
        forFieldF3_F2.add("F3_F2_val2");
        
        Map<String, Object> forField3 = new HashMap<>();
        forField3.put("F3_F1", "F3_F1_val");
        forField3.put("F3_F2", forFieldF3_F2);
        
        Map<String, Object> forField4_0 = new HashMap<>();
        forField4_0.put("F4_V_F1", "F4_V_F1_val");
        
        Map<String, Object> forField4_1 = new HashMap<>();
        forField4_1.put("F4_V_F2", "F4_V_F2_val");
        
        List<Object> forField4 = new ArrayList<>();
        forField4.add(forField4_0);
        forField4.add(forField4_1);
        
        Map<String, Object> data2 = new HashMap<>();
        data2.put("field1", "value1");
        data2.put("field2", forField2);
        data2.put("field3", forField3);
        data2.put("field4", forField4);
   
        Solr es = new Solr(client, "test_core");
        
        es.insert("key1", data1);
        
        IndexedDocument document1 = es.getDocument("key1");
        assertTrue(document1.hasData());
        
        es.insert("key2", data2);
        
        IndexedDocument document2 = es.getDocument("key2");
        assertTrue(document2.hasData());
        
        assertEquals(data2, document1.getData());
        assertEquals(data2, document2.getData());
    }
}
*/