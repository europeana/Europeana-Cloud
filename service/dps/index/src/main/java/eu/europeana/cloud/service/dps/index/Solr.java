package eu.europeana.cloud.service.dps.index;

import eu.europeana.cloud.service.dps.index.exception.ConnectionException;
import eu.europeana.cloud.service.dps.index.exception.IndexerException;
import eu.europeana.cloud.service.dps.index.structure.IndexedDocument;
import eu.europeana.cloud.service.dps.index.structure.IndexerInformations;
import eu.europeana.cloud.service.dps.index.structure.SearchHit;
import eu.europeana.cloud.service.dps.index.structure.SearchResult;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient.RemoteSolrException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.CursorMarkParams;
import org.apache.solr.common.params.MoreLikeThisParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.codehaus.jackson.map.ObjectMapper;

/**
 *
 * @author Pavel Kefurt <Pavel.Kefurt@gmail.com>
 */
public class Solr implements Indexer
{
    private static final String UNIQUE_KEY_FIELD = "id";
    
    private final IndexerInformations indexInformations;
    
    private final SolrClient client;
    
    private static final int PAGE_SIZE = 10;
    
    private static final int MAX_QUERY_TERMS = 1;
    private static final int MIN_TERM_FREQ = 1;
    private static final int MIN_DOC_FREQ = 1;
    private static final int MAX_DOC_FREQ = 1;
    private static final int MIN_WORD_LENGTH = 1;
    private static final int MAX_WORD_LENGTH = 1;

    public Solr(IndexerInformations ii) throws IndexerException 
    {
        this.indexInformations = ii;
        
        String address = ii.getAddresses().get(0);
        
        if(address == null)
        {
            throw new IndexerException("Empty address list.");
        }
        
        //determine which Solr server will be used
        if(address.matches("http://"))
        {
            if(address.charAt(address.length()-1) == '/')
            {
                client = new HttpSolrClient(address+ii.getIndex());
            }
            else
            {
                client = new HttpSolrClient(address+"/"+ii.getIndex());
            }
        }
        else
        {
            CloudSolrClient cClient= new CloudSolrClient(String.join(",", ii.getAddresses()));  
            cClient.setDefaultCollection(ii.getIndex());
            client = cClient;
        }
    }

    @Override
    public Object getIndexer() 
    {
        return client;
    }

    @Override
    public SupportedIndexers getIndexerName() 
    {
        return SupportedIndexers.SOLR_INDEXER;
    }

    @Override
    public IndexerInformations getIndexerInformations() 
    {
        return indexInformations;
    }

    @Override
    public SearchResult getMoreLikeThis(String documentId) throws IndexerException 
    {
        return getMoreLikeThis(documentId, null, MAX_QUERY_TERMS, MIN_TERM_FREQ, MIN_DOC_FREQ, 
                MAX_DOC_FREQ, MIN_WORD_LENGTH, MAX_WORD_LENGTH, PAGE_SIZE, 0);
    }
    
    @Override
    public SearchResult getMoreLikeThis(String documentId, int size, int timeout) throws IndexerException 
    {
        return getMoreLikeThis(documentId, null, MAX_QUERY_TERMS, MIN_TERM_FREQ, MIN_DOC_FREQ, 
                MAX_DOC_FREQ, MIN_WORD_LENGTH, MAX_WORD_LENGTH, size, timeout);
    }
    
    @Override
    public SearchResult getMoreLikeThis(String documentId, String[] fields) throws IndexerException 
    {
        return getMoreLikeThis(documentId, fields, MAX_QUERY_TERMS, MIN_TERM_FREQ, MIN_DOC_FREQ, 
                MAX_DOC_FREQ, MIN_WORD_LENGTH, MAX_WORD_LENGTH, PAGE_SIZE, 0);
    }
    
    @Override
    public SearchResult getMoreLikeThis(String documentId, String[] fields, int size, int timeout) throws IndexerException 
    {
        return getMoreLikeThis(documentId, fields, MAX_QUERY_TERMS, MIN_TERM_FREQ, MIN_DOC_FREQ, 
                MAX_DOC_FREQ, MIN_WORD_LENGTH, MAX_WORD_LENGTH, size, timeout);
    }

    @Override
    public SearchResult getMoreLikeThis(String documentId, String[] fields, int maxQueryTerms, int minTermFreq, 
            int minDocFreq, int maxDocFreq, int minWordLength, int maxWordLength, int size, int timeout) throws IndexerException 
    {
        List<String> f;
        if(fields == null)
        {
            f = new ArrayList<>();
            f.add("*");
        }
        else
        {
            f = Arrays.asList(fields);              
        }
        f.add("score");
        
        SolrQuery mlt = new SolrQuery();
        mlt.set(MoreLikeThisParams.MLT, true);
        mlt.set(MoreLikeThisParams.SIMILARITY_FIELDS, (String[]) f.toArray());
        mlt.set(MoreLikeThisParams.MAX_QUERY_TERMS, maxQueryTerms);
        mlt.set(MoreLikeThisParams.MIN_TERM_FREQ, minTermFreq);
        mlt.set(MoreLikeThisParams.MIN_DOC_FREQ, minDocFreq);
        mlt.set(MoreLikeThisParams.MAX_DOC_FREQ,maxDocFreq);
        mlt.set(MoreLikeThisParams.MIN_WORD_LEN, minWordLength);
        mlt.set(MoreLikeThisParams.MAX_WORD_LEN, maxWordLength);
        mlt.setQuery(UNIQUE_KEY_FIELD+":"+documentId);
        mlt.setRows(size);
        
        if(timeout > 0)
        {
            mlt.set(CursorMarkParams.CURSOR_MARK_PARAM, CursorMarkParams.CURSOR_MARK_START);
        } 
        
        QueryResponse response;
        try 
        {
            response = client.query(mlt);
        } 
        catch (SolrServerException ex) 
        {
            throw new ConnectionException(ex);
        } 
        catch (RemoteSolrException | IOException ex) 
        {
            throw new IndexerException(ex);
        }
        
        SearchResult res=  fromQueryResponseToSearchResult(response);
        res.setQueryType(SearchResult.QueryTypes.MORE_LIKE_THIS);
        res.setQuery(response.getResponseHeader().get("params"));
        
        return res;
    }

    @Override
    public SearchResult search(String text, IndexFields[] fields) throws IndexerException 
    {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public SearchResult search(String text, IndexFields[] fields, int size, int timeout) throws IndexerException 
    {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public SearchResult search(String text, String[] fields) throws IndexerException 
    {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public SearchResult search(String text, String[] fields, int size, int timeout) throws IndexerException 
    {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public SearchResult searchFullText(String text) throws IndexerException 
    {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public SearchResult searchFullText(String text, int size, int timeout) throws IndexerException 
    {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void insert(String data) throws IndexerException 
    {
        Map<String, Object> tmp;
        try 
        {
            tmp = new ObjectMapper().readValue(data, HashMap.class);
        } 
        catch (IOException ex) 
        {
            throw new IndexerException(ex);
        }
        
        UUID id = UUID.randomUUID();
        
        insert(id.toString(), tmp);
    }

    @Override
    public void insert(Map<String, Object> data) throws IndexerException 
    {
        UUID id = UUID.randomUUID();
        
        insert(id.toString(), data);
    }

    @Override
    public void insert(String documentId, String data) throws IndexerException 
    {
        Map<String, Object> tmp;
        try 
        {
            tmp = new ObjectMapper().readValue(data, HashMap.class);
        } 
        catch (IOException ex) 
        {
            throw new IndexerException(ex);
        }
        
        insert(documentId, tmp);
    }

    @Override
    public void insert(String documentId, Map<String, Object> data) throws ConnectionException, IndexerException 
    {
        SolrInputDocument document = new SolrInputDocument();
        document.addField(UNIQUE_KEY_FIELD, documentId);
        
        for(Map.Entry<String, Object> field: data.entrySet())
        {
            if(field.getValue() instanceof Collection)
            {
                //field values can be complex but solr needs simple list => value is store as JSON
                List<String> values = new ArrayList();
                for(Object o: (Collection<?>)field.getValue())
                {
                    try 
                    {
                        values.add(new ObjectMapper().writeValueAsString(o));
                    } 
                    catch (IOException ex) 
                    {
                        values.add(o.toString());
                    }
                }
                
                document.addField(field.getKey(), values);
            }
            else    //store as one value (string)
            {
                String val;
                try 
                {
                    val = new ObjectMapper().writeValueAsString(field.getValue());
                } 
                catch (IOException ex) 
                {
                    val = field.getValue().toString();
                }
                
                document.addField(field.getKey(), val);
            }
        }
        
        try 
        {
            client.add(document);
            client.commit();
        } 
        catch (SolrServerException ex) 
        {
            throw new ConnectionException(ex);
        } 
        catch (RemoteSolrException | IOException ex) 
        {
            throw new IndexerException(ex);
        }
    }

    @Override
    public void update(String documentId, String data) throws IndexerException 
    {
        Map<String, Object> tmp;
        try 
        {
            tmp = new ObjectMapper().readValue(data, HashMap.class);
        } 
        catch (IOException ex) 
        {
            throw new IndexerException(ex);
        }
        
        insert(documentId, tmp);
    }

    @Override
    public void update(String documentId, Map<String, Object> data) throws IndexerException 
    {
        insert(documentId, data);
    }

    @Override
    public IndexedDocument getDocument(String documentId) throws ConnectionException, IndexerException 
    {
        SolrDocument response;
        try 
        {
            response = client.getById(documentId);
        } 
        catch (SolrServerException ex) 
        {
            throw new ConnectionException(ex);
        } 
        catch (RemoteSolrException | IOException ex) 
        {
            throw new IndexerException(ex);
        }
        
        return fromSolrDocumentToIndexedDocument(response);
    }
    
    @Override
    public SearchResult getNextPage(String scrollId, Object context) throws ConnectionException, IndexerException 
    {
        if(scrollId == null)
        {
            return null;
        }
        
        SearchResult.QueryTypes qType = SearchResult.QueryTypes.UNKNOWN;
        NamedList<Object> nl;
        
        if(context != null)
        {
            if(context instanceof SearchResult)
            {
                SearchResult sr = (SearchResult)context;
                
                qType = sr.getQueryType();
                
                nl = (NamedList<Object>) sr.getQuery();   
            }
            else if(context instanceof NamedList)
            {
                nl = (NamedList<Object>) context;
            }
            else
            {
                return null;
            }
        }
        else
        {
            return null;
        }
        
        nl.add(CursorMarkParams.CURSOR_MARK_PARAM, scrollId);

        QueryResponse response;
        try 
        {              
            response = client.query(SolrParams.toSolrParams(nl));
        } 
        catch (SolrServerException ex) 
        {
            throw new ConnectionException(ex);
        } 
        catch (RemoteSolrException | IOException ex) 
        {
            throw new IndexerException(ex);
        }
        
        String nextScrollId = response.getNextCursorMark();
        
        if(scrollId.equals(nextScrollId))
        {
            nextScrollId = null;
        }
        
        SearchResult res = fromQueryResponseToSearchResult(response);
        res.setQueryType(qType);
        res.setQuery(response.getResponseHeader().get("params"));
        res.setScrollId(nextScrollId);
        
        return res;
    }
    
    private IndexedDocument fromSolrDocumentToIndexedDocument(SolrDocument solrDocument)
    {   
        Map<String, Object> data = getDataFromSolrDocument(solrDocument);
        
        IndexedDocument newDocument = new IndexedDocument(indexInformations, solrDocument.getFirstValue(UNIQUE_KEY_FIELD).toString(), 
                Long.parseLong(solrDocument.getFirstValue("_version_").toString()));
        newDocument.setData(data);
        
        return newDocument;
    } 
    
    private SearchResult fromQueryResponseToSearchResult(QueryResponse response)
    {       
        SolrDocumentList results = response.getResults();
        
        List<SearchHit> hits = new ArrayList();
        for(SolrDocument document: results)
        {
            Map<String, Object> data = getDataFromSolrDocument(document);
        
            hits.add(new SearchHit(indexInformations, document.getFirstValue(UNIQUE_KEY_FIELD).toString(), 
                    Long.parseLong(document.getFirstValue("_version_").toString()),
                    Float.parseFloat(document.getFirstValue("score").toString()), data));
        }      
        
        return new SearchResult(hits, results.getNumFound(), results.getMaxScore(), 
                response.getQTime(), response.getNextCursorMark());   
    }
    
    
    private Map<String, Object> getDataFromSolrDocument(SolrDocument document)
    {
        Map<String, Object> data = new HashMap<>();
        
        Map<String, Collection<Object>> values = document.getFieldValuesMap();
        
        for(String name: document.getFieldNames())
        {
            Collection<Object> values_ = values.get(name);
            if(values.size() == 1)
            {
                data.put(name, values_.toArray()[0].toString());
            }
            else
            {
                for(Object value: values_)
                {           
                    data.put(name, value.toString());
                }
            }
        }
        
        return data;
    }
}
