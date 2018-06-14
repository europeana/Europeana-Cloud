package eu.europeana.cloud.service.dps.index;

import eu.europeana.cloud.service.dps.index.exception.IndexerException;
import eu.europeana.cloud.service.dps.index.structure.IndexedDocument;
import eu.europeana.cloud.service.dps.index.structure.SearchResult;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.hamcrest.ElasticsearchAssertions;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;


/**
 * Test for Elasticsearch indexer.
 * @author Pavel Kefurt <Pavel.Kefurt@gmail.com>
 */
@ElasticsearchIntegrationTest.ClusterScope(scope = ElasticsearchIntegrationTest.Scope.TEST, numClientNodes = 6)
public class ElasticsearchTest extends ElasticsearchIntegrationTest
{  
    public static Client client()
    {
        return internalCluster().client();
    }
    
    @Test
    public void searchTest() throws IndexerException
    {
        boolean ok = false;
        int i = 0;
        do
        {
            try
            {
                createIndex("test");

                client().prepareIndex("test", "type", "1").setSource("field1", "value1").execute().actionGet();
                client().prepareIndex("test", "type", "2").setSource("field1", "value2").execute().actionGet();
                client().prepareIndex("test", "type", "3").setSource(IndexFields.RAW_TEXT, "some full text with lot of informations").execute().actionGet();
                client().prepareIndex("test", "type", "4").setSource(IndexFields.RAW_TEXT, "dog is an animal").execute().actionGet();
                client().prepareIndex("test", "type", "5").setSource("field1", "value3", "field2", "value1 and value2").execute().actionGet();
                client().prepareIndex("test", "type", "6").setSource("field1", "value4", "field2", "value5").execute().actionGet();
                client().prepareIndex("test", "type", "7").setSource("field3", "my phrase").execute().actionGet();
                client().prepareIndex("test", "type", "8").setSource("field3", "my dog and phrase").execute().actionGet();
                client().prepareIndex("test", "type", "9").setSource("field3", "my dog and cat").execute().actionGet();
                client().prepareIndex("test", "type", "10").setSource("field3", "my dog and your cat").execute().actionGet();
                client().prepareIndex("test", "type", "11").setSource("field3", "my cat and dog").execute().actionGet();
                refresh();

                Elasticsearch es = new Elasticsearch(client(), "test", "type");

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

                result = es.searchFullText("cat is an animal");
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

                ok = true;
                break;
            }
            catch(Exception ex)
            {
                i++;
            }
        }while(i<4);
        
        if(!ok)
        {
            fail();
        }
    }
    
    @Test
    public void moreLikeThisTest() throws IndexerException, IOException
    {
        boolean ok = false;
        int i = 0;
        do
        {
            try
            {
                XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("type1").startObject("properties")
                        .startObject("field3").field("type", "string").field("term_vector", "yes")
                        .endObject().endObject().endObject().endObject();

                ElasticsearchAssertions.assertAcked(prepareCreate("test").addMapping("type1", mapping));

                //ensureGreen(TimeValue.timeValueSeconds(60));
                client().prepareIndex("test", "type1", "7").setSource("field3", "my phrase").execute().actionGet();
                client().prepareIndex("test", "type1", "8").setSource("field3", "my dog and phrase").execute().actionGet();
                client().prepareIndex("test", "type1", "9").setSource("field3", "my dog and cat").execute().actionGet();
                client().prepareIndex("test", "type1", "10").setSource("field3", "my dog and your cat").execute().actionGet();
                client().prepareIndex("test", "type1", "11").setSource("field3", "my cat and dog").execute().actionGet();
                client().prepareIndex("test", "type1", "12").setSource("field3", "another text").execute().actionGet();
                refresh();

                Elasticsearch es = new Elasticsearch(client(), "test", "type1");

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

                ok=true;
                break;
            }
            catch(Exception ex)
            {
                i++;
            }
        }while(i<4);
        
        if(!ok)
        {
            fail();
        }
    }
    
    @Test
    public void scrollTest() throws IndexerException
    {
        boolean ok = false;
        int i = 0;
        do
        {
            try
            {
                createIndex("test");
                //ensureGreen(TimeValue.timeValueSeconds(60));
                client().prepareIndex("test", "type", "1").setSource("field1", "value1").execute().actionGet();
                client().prepareIndex("test", "type", "2").setSource("field1", "value2").execute().actionGet();
                client().prepareIndex("test", "type", "5").setSource("field1", "value3", "field2", "value1 and value2").execute().actionGet();
                client().prepareIndex("test", "type", "6").setSource("field1", "value4", "field2", "value5").execute().actionGet();
                refresh();

                Elasticsearch es = new Elasticsearch(client(), "test", "type");

                SearchResult result;

                String[] fields2 = {"field1", "field2"};
                result = es.search("value2", fields2, 1, 5000);       
                assertTrue(result.getTotalHits() == 2);
                assertTrue(result.getHits().size() == 1);

                result = es.getNextPage(result.getScrollId(), result);
                assertTrue(result.getTotalHits() == 2);
                assertTrue(result.getHits().size() == 1);

                result = es.getNextPage(result.getScrollId(), result);
                assertNull(result);  
        
                ok=true;
                break;
            }
            catch(Exception ex)
            {
                i++;
            }
        }while(i<4);
        
        if(!ok)
        {
            fail();
        }
    }
 
    @Test
    public void manipulationTest() throws IndexerException
    {
        boolean ok = false;
        int i = 0;
        do
        {
            try
            {
                createIndex("test");
                //ensureGreen(TimeValue.timeValueSeconds(120));
                client().prepareIndex("test", "type", "1").setSource("field1", "value1").execute().actionGet();
                refresh();

                Elasticsearch es = new Elasticsearch(client(), "test", "type");

                //----
                Map<String, Object> data = new HashMap<>();
                data.put("field2", "value2");
                data.put("field1", "value99");
                es.insert(data);
                es.insert("keyX", data);
                refresh();

                SearchResponse response = client().prepareSearch().setQuery(QueryBuilders.matchAllQuery()).get();
                assertTrue(response.getHits().getTotalHits() == 3);

                //----
                Map<String, Object> data2 = new HashMap<>();
                data.put("field1", "value9");
                es.update("keyX", data);
                refresh();

                response = client().prepareSearch().setQuery(QueryBuilders.matchAllQuery()).get();
                assertTrue(response.getHits().getTotalHits() == 3);
                for(org.elasticsearch.search.SearchHit hit: response.getHits().getHits())
                {
                    if("keyX".equals(hit.getId()))
                    {
                        assertEquals("value9", hit.getSource().get("field1"));
                        assertEquals("value2", hit.getSource().get("field2"));
                        break;
                    }
                }

                //----
                es.update("keyX", (String)null);
                refresh();

                response = client().prepareSearch().setQuery(QueryBuilders.matchAllQuery()).get();
                assertTrue(response.getHits().getTotalHits() == 3);
                for(org.elasticsearch.search.SearchHit hit: response.getHits().getHits())
                {
                    if("keyX".equals(hit.getId()))
                    {
                        assertEquals("value9", hit.getSource().get("field1"));
                        assertEquals("value2", hit.getSource().get("field2"));
                        break;
                    }
                }

                //----
                es.update("keyX", (Map)null);
                refresh();

                response = client().prepareSearch().setQuery(QueryBuilders.matchAllQuery()).get();
                assertTrue(response.getHits().getTotalHits() == 3);
                for(org.elasticsearch.search.SearchHit hit: response.getHits().getHits())
                {
                    if("keyX".equals(hit.getId()))
                    {
                        assertEquals("value9", hit.getSource().get("field1"));
                        assertEquals("value2", hit.getSource().get("field2"));
                        break;
                    }
                }

                //----
                es.update("keyX", new HashMap<String, Object>());
                refresh();

                response = client().prepareSearch().setQuery(QueryBuilders.matchAllQuery()).get();
                assertTrue(response.getHits().getTotalHits() == 3);
                for(org.elasticsearch.search.SearchHit hit: response.getHits().getHits())
                {
                    if("keyX".equals(hit.getId()))
                    {
                        assertEquals("value9", hit.getSource().get("field1"));
                        assertEquals("value2", hit.getSource().get("field2"));
                        break;
                    }
                }

                //----
                es.update("keyX", "");
                refresh();

                response = client().prepareSearch().setQuery(QueryBuilders.matchAllQuery()).get();
                assertTrue(response.getHits().getTotalHits() == 3);
                for(org.elasticsearch.search.SearchHit hit: response.getHits().getHits())
                {
                    if("keyX".equals(hit.getId()))
                    {
                        assertEquals("value9", hit.getSource().get("field1"));
                        assertEquals("value2", hit.getSource().get("field2"));
                        break;
                    }
                }

                //----
                es.delete("1");
                refresh();
                response = client().prepareSearch().setQuery(QueryBuilders.matchAllQuery()).get();
                assertTrue(response.getHits().getTotalHits() == 2);

                //----
                es.delete(null);
                refresh();
                response = client().prepareSearch().setQuery(QueryBuilders.matchAllQuery()).get();
                assertTrue(response.getHits().getTotalHits() == 2);

                //----
                es.delete("");
                refresh();
                response = client().prepareSearch().setQuery(QueryBuilders.matchAllQuery()).get();
                assertTrue(response.getHits().getTotalHits() == 2);

                //----
                es.insert(null, data);
                refresh();

                response = client().prepareSearch().setQuery(QueryBuilders.matchAllQuery()).get();
                assertTrue(response.getHits().getTotalHits() == 3);

                //----
                es.insert("2", new HashMap<String, Object>());
                refresh();

                response = client().prepareSearch().setQuery(QueryBuilders.matchAllQuery()).get();
                assertTrue(response.getHits().getTotalHits() == 3);

                //----
                es.insert("3", "");
                refresh();

                response = client().prepareSearch().setQuery(QueryBuilders.matchAllQuery()).get();
                assertTrue(response.getHits().getTotalHits() == 3);

                //----
                es.insert("4", (String)null);
                refresh();

                response = client().prepareSearch().setQuery(QueryBuilders.matchAllQuery()).get();
                assertTrue(response.getHits().getTotalHits() == 3);

                //----
                es.insert("4", (Map)null);
                refresh();

                response = client().prepareSearch().setQuery(QueryBuilders.matchAllQuery()).get();
                assertTrue(response.getHits().getTotalHits() == 3);

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
                
                ok=true;
                break;
            }
            catch(Exception ex)
            {
                i++;
            }
        }while(i<4);
        
        if(!ok)
        {
            fail();
        }
    }
    
    @Test
    public void complexDataManipulationTest() throws IndexerException
    {
        boolean ok = false;
        int i = 0;
        do
        {
            try
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

                Elasticsearch es = new Elasticsearch(client(), "test", "type");

                es.insert("key1", data1);
                refresh();

                IndexedDocument document1 = es.getDocument("key1");
                assertTrue(document1.hasData());

                es.insert("key2", data2);
                refresh();

                IndexedDocument document2 = es.getDocument("key2");
                assertTrue(document2.hasData());

                assertEquals(data2, document1.getData());
                assertEquals(data2, document2.getData());
        
                ok=true;
                break;
            }
            catch(Exception ex)
            {
                i++;
            }
        }while(i<4);
        
        if(!ok)
        {
            fail();
        }
    }
}
