package eu.europeana.cloud.service.dps.index;

import eu.europeana.cloud.service.dps.index.exception.ConnectionException;
import eu.europeana.cloud.service.dps.index.exception.IndexerException;
import eu.europeana.cloud.service.dps.index.structure.IndexedDocument;
import eu.europeana.cloud.service.dps.index.structure.IndexerInformations;
import eu.europeana.cloud.service.dps.index.structure.SearchHit;
import eu.europeana.cloud.service.dps.index.structure.SearchResult;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.UUID;
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
 * Solr indexer.
 * @author Pavel Kefurt <Pavel.Kefurt@gmail.com>
 */
public class Solr implements Indexer
{
    private static final String UNIQUE_KEY_FIELD = "id";
    
    private final IndexerInformations indexInformations;
    
    private final SolrClient client;

    public Solr(IndexerInformations ii) throws IndexerException 
    {
        this.indexInformations = ii;
        
        List<String> addresses = ii.getAddresses();
        String address = addresses.isEmpty() ? null : addresses.get(0);
        
        if(address == null)
        {
            throw new IndexerException("Empty address list.");
        }

        //determine which Solr server will be used
        if(address.matches("http://.*"))
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
            CloudSolrClient cClient= new CloudSolrClient(String.join(",", addresses));  
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
    public SearchResult getMoreLikeThis(String documentId) throws ConnectionException, IndexerException 
    {
        return getMoreLikeThis(documentId, null, MAX_QUERY_TERMS, MIN_TERM_FREQ, MIN_DOC_FREQ, 
                MAX_DOC_FREQ, MIN_WORD_LENGTH, MAX_WORD_LENGTH, PAGE_SIZE, 0, false);
    }
    
    @Override
    public SearchResult getMoreLikeThis(String documentId, int size, int timeout) throws ConnectionException, IndexerException 
    {
        return getMoreLikeThis(documentId, null, MAX_QUERY_TERMS, MIN_TERM_FREQ, MIN_DOC_FREQ, 
                MAX_DOC_FREQ, MIN_WORD_LENGTH, MAX_WORD_LENGTH, size, timeout, false);
    }
    
    @Override
    public SearchResult getMoreLikeThis(String documentId, String[] fields) throws ConnectionException, IndexerException 
    {
        return getMoreLikeThis(documentId, fields, MAX_QUERY_TERMS, MIN_TERM_FREQ, MIN_DOC_FREQ, 
                MAX_DOC_FREQ, MIN_WORD_LENGTH, MAX_WORD_LENGTH, PAGE_SIZE, 0, false);
    }
    
    @Override
    public SearchResult getMoreLikeThis(String documentId, String[] fields, int size, int timeout) 
            throws ConnectionException, IndexerException 
    {
        return getMoreLikeThis(documentId, fields, MAX_QUERY_TERMS, MIN_TERM_FREQ, MIN_DOC_FREQ, 
                MAX_DOC_FREQ, MIN_WORD_LENGTH, MAX_WORD_LENGTH, size, timeout, false);
    }

    @Override
    public SearchResult getMoreLikeThis(String documentId, String[] fields, int maxQueryTerms, int minTermFreq, 
            int minDocFreq, int maxDocFreq, int minWordLength, int maxWordLength, int size, int timeout, Boolean includeItself)
            throws ConnectionException, IndexerException 
    {  
        String[] keys = 
        {
            MoreLikeThisParams.MAX_QUERY_TERMS,
            MoreLikeThisParams.MIN_TERM_FREQ,
            MoreLikeThisParams.MIN_DOC_FREQ,
            MoreLikeThisParams.MAX_DOC_FREQ,
            MoreLikeThisParams.MIN_WORD_LEN,
            MoreLikeThisParams.MAX_WORD_LEN     
        };
        
        int[] values = 
        {
            maxQueryTerms,
            minTermFreq,
            minDocFreq,
            maxDocFreq,
            minWordLength,
            maxWordLength         
        };
        
        assert(keys.length == values.length);
                
        SolrQuery mlt = new SolrQuery();
        mlt.set(MoreLikeThisParams.MLT, true);
        mlt.set(MoreLikeThisParams.MATCH_INCLUDE, includeItself);
        mlt.setQuery(UNIQUE_KEY_FIELD+":\""+documentId+"\"");
        mlt.setRows(size);
        mlt.setIncludeScore(true);       
        
        for(int i=0; i<keys.length;i++)
        {
            if(values[i] != -1)
            {
                mlt.set(keys[i], values[i]);
            }
        }
        
        if(fields != null)
        {
            for(int i=0; i<fields.length; i++)
            {
                if("_all".equals(fields[i]))
                {
                    fields[i]="*";
                    break;
                }
            }
            
            mlt.set(MoreLikeThisParams.SIMILARITY_FIELDS, fields);
        }
        else
        {
            mlt.set(MoreLikeThisParams.SIMILARITY_FIELDS, "*");
        }
  
        if(timeout > 0)
        {
            mlt.set(CursorMarkParams.CURSOR_MARK_PARAM, CursorMarkParams.CURSOR_MARK_START);
            
            List<SolrQuery.SortClause> sorts = new ArrayList();
            sorts.add(new SolrQuery.SortClause("score", SolrQuery.ORDER.desc));
            sorts.add(new SolrQuery.SortClause(UNIQUE_KEY_FIELD, SolrQuery.ORDER.asc)); //sort by unique key is required
            mlt.setSorts(sorts);
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
        
        SearchResult res = fromQueryResponseToSearchResult(response);
        
        //match_include not works => do that itself
        if(!includeItself)
        {
            for(SearchHit sh: res.getHits())
            {
                if(documentId.equals(sh.getId()))
                {
                    if(res.getTotalHits() == 1)
                    {
                        res = new SearchResult(null, 0, 0, -1);
                    }
                    else
                    {
                        res.getHits().remove(sh);
                        float newScore = res.getHits().get(0).getScore();
                        res = new SearchResult(res.getHits(), res.getTotalHits()-1, newScore, res.getTookTime(), res.getScrollId());
                    }
                    break;
                }
            }
        }
        
        res.setQuery(response.getResponseHeader().get("params"));
        
        return res;
    }

    @Override
    public SearchResult search(String text, String[] fields) throws ConnectionException, IndexerException 
    {
        return search(text, fields, PAGE_SIZE, 0);
    }

    @Override
    public SearchResult search(String text, String[] fields, int size, int timeout) 
            throws ConnectionException, IndexerException 
    {
        StringJoiner queryString = new StringJoiner(" "+Operator.OR+" ");
        
        for(String field: fields)
        {
            if("_all".equals(field))
            {
                queryString = new StringJoiner("").add("*:("+text+")");
                break;
            }
            
            queryString.add(field+":("+text+")");
        } 
        
        if(queryString.length() == 0)
        {
            new StringJoiner("").add("*:("+text+")");
        }
        
        SolrQuery query = new SolrQuery();
        query.setQuery(queryString.toString());   
        query.setRows(size);
        query.setIncludeScore(true);
        
        if(timeout > 0)
        {
            query.set(CursorMarkParams.CURSOR_MARK_PARAM, CursorMarkParams.CURSOR_MARK_START);
            
            List<SolrQuery.SortClause> sorts = new ArrayList();
            sorts.add(new SolrQuery.SortClause("score", SolrQuery.ORDER.desc));
            sorts.add(new SolrQuery.SortClause(UNIQUE_KEY_FIELD, SolrQuery.ORDER.asc)); //sort by unique key is required
            query.setSorts(sorts);
        } 
        
        QueryResponse response;
        try 
        {
            response = client.query(query);
        } 
        catch (SolrServerException ex) 
        {
            throw new ConnectionException(ex);
        } 
        catch (RemoteSolrException | IOException ex) 
        {
            throw new IndexerException(ex);
        }
        
        SearchResult res = fromQueryResponseToSearchResult(response);
        res.setQuery(response.getResponseHeader().get("params"));
        
        return res;
    }

    @Override
    public SearchResult searchFullText(String text) throws ConnectionException, IndexerException 
    {
        String[] field = {IndexFields.RAW_TEXT.toString()};
        return search(text, field, PAGE_SIZE, 0);
    }

    @Override
    public SearchResult searchFullText(String text, int size, int timeout) throws ConnectionException, IndexerException 
    {
        String[] field = {IndexFields.RAW_TEXT.toString()};
        return search(text, field, size, timeout);
    }
    
    @Override
    public SearchResult searchPhraseInFullText(String text, int slop) throws ConnectionException, IndexerException 
    {
        return searchPhrase(text, IndexFields.RAW_TEXT.toString(), slop, PAGE_SIZE, 0);
    }

    @Override
    public SearchResult searchPhrase(String text, String field, int slop) throws ConnectionException, IndexerException 
    {
        return searchPhrase(text, field, slop, PAGE_SIZE, 0);
    }

    @Override
    public SearchResult searchPhrase(String text, String field, int slop, int size, int timeout) 
            throws ConnectionException, IndexerException 
    {      
        String q = field+":\""+text+"\"~"+slop;
               
        SolrQuery query = new SolrQuery();
        query.setQuery(q);   
        query.setRows(size);
        query.setIncludeScore(true);
        
        if(timeout > 0)
        {
            query.set(CursorMarkParams.CURSOR_MARK_PARAM, CursorMarkParams.CURSOR_MARK_START);
            
            List<SolrQuery.SortClause> sorts = new ArrayList();
            sorts.add(new SolrQuery.SortClause("score", SolrQuery.ORDER.desc));
            sorts.add(new SolrQuery.SortClause(UNIQUE_KEY_FIELD, SolrQuery.ORDER.asc)); //sort by unique key is required
            query.setSorts(sorts);
        } 
        
        QueryResponse response;
        try 
        {
            response = client.query(query);
        } 
        catch (SolrServerException ex) 
        {
            throw new ConnectionException(ex);
        } 
        catch (RemoteSolrException | IOException ex) 
        {
            throw new IndexerException(ex);
        }
        
        SearchResult res = fromQueryResponseToSearchResult(response);
        res.setQuery(response.getResponseHeader().get("params"));
        
        return res;
    }

    @Override
    public SearchResult advancedSearch(String query) throws IndexerException 
    {
        return advancedSearch(query, null, PAGE_SIZE, 0);
    }

    @Override
    public SearchResult advancedSearch(String query, int size, int timeout) throws IndexerException 
    {
        return advancedSearch(query, null, size, timeout);
    }

    @Override
    public SearchResult advancedSearch(String query, Map<String, Object> parameters) throws IndexerException 
    {
        return advancedSearch(query, parameters, PAGE_SIZE, 0);
    }

    @Override
    public SearchResult advancedSearch(String query, Map<String, Object> parameters, int size, int timeout) throws IndexerException 
    {
        SolrQuery q = new SolrQuery();
             
        if(parameters != null)
        {
            for(Map.Entry<String, Object> parameter: parameters.entrySet())
            {
                //TODO: set parameters is not possible without a new parser in solr server
                //q.set("", "");
            }
        }

        q.setQuery(query);
        q.setRows(size);
        q.setIncludeScore(true);
        
        if(timeout > 0)
        {
            q.set(CursorMarkParams.CURSOR_MARK_PARAM, CursorMarkParams.CURSOR_MARK_START);
            
            List<SolrQuery.SortClause> sorts = new ArrayList();
            sorts.add(new SolrQuery.SortClause("score", SolrQuery.ORDER.desc));
            sorts.add(new SolrQuery.SortClause(UNIQUE_KEY_FIELD, SolrQuery.ORDER.asc)); //sort by unique key is required
            q.setSorts(sorts);
        } 
        
        QueryResponse response;
        try 
        {
            response = client.query(q);
        } 
        catch (SolrServerException ex) 
        {
            throw new ConnectionException(ex);
        } 
        catch (RemoteSolrException | IOException ex) 
        {
            throw new IndexerException(ex);
        }
        
        SearchResult res = fromQueryResponseToSearchResult(response);
        res.setQuery(response.getResponseHeader().get("params"));
        
        return res;
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
            Object fieldValue = field.getValue();
            if(fieldValue instanceof Collection)
            {
                //field values can be complex but solr needs simple list => value is store as JSON string
                List<String> values = new ArrayList();
                for(Object o: (Collection<?>)fieldValue)
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
                    val = new ObjectMapper().writeValueAsString(fieldValue);
                } 
                catch (IOException ex) 
                {
                    val = fieldValue.toString();
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
    public void delete(String documentId) throws ConnectionException, IndexerException 
    {      
        try 
        {
            client.deleteById(documentId);
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
        NamedList<Object> nl;
        
        if(context != null)
        {
            if(context instanceof SearchResult)
            {
                SearchResult sr = (SearchResult)context;
                
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
        
        nl.remove(CursorMarkParams.CURSOR_MARK_PARAM);  //remove old cursor mark
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
        res.setQuery(response.getResponseHeader().get("params"));
        res.setScrollId(nextScrollId);
        
        return res;
    }
    
    private IndexedDocument fromSolrDocumentToIndexedDocument(SolrDocument solrDocument)
    {   
        if(solrDocument == null)
        {
            return null;
        }
        
        Map<String, Object> data = getDataFromSolrDocument(solrDocument);
        
        IndexedDocument newDocument = new IndexedDocument(indexInformations, solrDocument.getFirstValue(UNIQUE_KEY_FIELD).toString(), 
                Long.parseLong(solrDocument.getFirstValue("_version_").toString()));
        newDocument.setData(data);
        
        return newDocument;
    } 
    
    private SearchResult fromQueryResponseToSearchResult(QueryResponse response)
    {        
        SolrDocumentList results = response != null ? response.getResults() : null;
        
        if(results == null)
        {
            return new SearchResult(null, 0, 0, -1);
        }
        
        assert(response != null);
        
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
        if(document == null)
        {
            return null;
        }
        
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
        
        //remove no data fields
        data.remove("_version_");
        data.remove(UNIQUE_KEY_FIELD);
        data.remove("score");
        
        return data;
    }
}
