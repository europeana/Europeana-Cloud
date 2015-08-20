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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Solr indexer.
 * @author Pavel Kefurt <Pavel.Kefurt@gmail.com>
 */
public class Solr implements Indexer
{
    public static final String UNIQUE_KEY_FIELD = "id";
    
    private final IndexerInformations indexInformations;
    
    private final SolrClient client;
    
    private final Map<String, String[]> allFieldsWithOptionOrAll = new HashMap<>();
    
    private static final Logger LOGGER = LoggerFactory.getLogger(Solr.class);

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
    
    public Solr(String clasterAddresses, String index) throws IndexerException 
    {
        this(new IndexerInformations(SupportedIndexers.SOLR_INDEXER.name(), index, "", clasterAddresses));
    }
    
    protected Solr(SolrClient client, String index)
    {
        this.client = client;
        
        indexInformations = new IndexerInformations(SupportedIndexers.SOLR_INDEXER.name(), index, "");
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
        if(documentId == null || documentId.isEmpty() || size < 1)
        {
            return new SearchResult(null, 0, 0, -1);
        }
        
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
                
        SolrQuery mlt = new SolrQuery(UNIQUE_KEY_FIELD+":\""+documentId+"\"");
        mlt.set(MoreLikeThisParams.MLT, true);
        mlt.set(MoreLikeThisParams.MATCH_INCLUDE, includeItself);   //TODO: it doesn`t work. WHY?!
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
            for (String field : fields) 
            {
                if ("_all".equals(field)) 
                {
                    fields = getAllFieldsWithOptionOrAll("termVectors");
                    break;
                }
            }
            
            mlt.set(MoreLikeThisParams.SIMILARITY_FIELDS, fields);
        }
        else
        {
            mlt.set(MoreLikeThisParams.SIMILARITY_FIELDS, getAllFieldsWithOptionOrAll("termVectors"));
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
        
        
        if(response == null)
        {
            return new SearchResult(null, 0, 0, -1);
        }
   
        SolrDocumentList results = new SolrDocumentList();
        
        NamedList<Object> mltResponse = (NamedList<Object>)response.getResponse().get("moreLikeThis");
        if(mltResponse.size() > 0)
        {
            results = (SolrDocumentList)mltResponse.getVal(0);

            if(includeItself)
            {
                //check if reference document is present (MoreLikeThisParams.MATCH_INCLUDE works)
                boolean present = false;
                for(SolrDocument d: results)
                {
                    if(d.get(UNIQUE_KEY_FIELD) == documentId)
                    {
                        present = true;
                        break;
                    }
                }
                
                if(!present)
                {                    
                    SolrDocumentList tmpList = response.getResults();
                    if(tmpList != null && !tmpList.isEmpty())
                    {
                        //find reference document
                        SolrDocument tmpDoc = null;
                        for(SolrDocument d: tmpList)
                        {
                            if(documentId.equals(d.get(UNIQUE_KEY_FIELD)))
                            {
                                tmpDoc = d;
                                break;
                            }
                        }

                        //add reference document to result list
                        if(tmpDoc != null)
                        {
                            results.add(tmpDoc);
                            float score = Float.parseFloat(tmpDoc.getFirstValue("score").toString());
                            if(score > results.getMaxScore())
                            {
                                results.setMaxScore(score);
                            }
                            results.setNumFound(results.getNumFound()+1);
                        }
                    }
                }
            }
        }
        
        
        List<SearchHit> hits = new ArrayList();
        for(SolrDocument document: results)
        {
            Map<String, Object> data = getDataFromSolrDocument(document);
            long version = (long)-1;
            if(document.getFirstValue("_version_") != null)
            {
                version = Long.parseLong(document.getFirstValue("_version_").toString());
            }
            
            hits.add(new SearchHit(indexInformations, document.getFirstValue(UNIQUE_KEY_FIELD).toString(), 
                    version, Float.parseFloat(document.getFirstValue("score").toString()), data));
        }      
        
        SearchResult res = new SearchResult(hits, results.getNumFound(), results.getMaxScore(), 
                response.getQTime(), response.getNextCursorMark());  
 
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
        if(text == null || fields == null || size < 1)
        {
            return new SearchResult(null, 0, 0, -1);
        }
        
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
        if(text == null || field == null || slop < 0 || size < 1)
        {
            return new SearchResult(null, 0, 0, -1);
        }
        
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
        if(query == null || size < 1)
        {
            return new SearchResult(null, 0, 0, -1);
        }
        
        SolrQuery q = new SolrQuery();
             
        if(parameters != null)
        {
            for(Map.Entry<String, Object> parameter: parameters.entrySet())
            {
                String key = parameter.getKey();
                if("default_field".equals(key))
                {
                    q.set("df", parameter.getValue().toString());
                }
                else if("default_operator".equals(key))
                {
                    q.set("q.op", parameter.getValue().toString());
                }
                
                //TODO: set another parameters is not possible without a new parser in solr server
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
        if(data == null || data.isEmpty())
        {
            return;
        }
        
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
        if(data == null || data.isEmpty())
        {
            return;
        }
        
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
        if(documentId == null || documentId.isEmpty())
        {
            documentId = UUID.randomUUID().toString();
        }
        
        SolrInputDocument document = toSolrInputDocument(data);
        if(document == null)
        {
            return;
        }
        document.addField(UNIQUE_KEY_FIELD, documentId);      
        
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
        if(data == null || data.isEmpty())
        {
            return;
        }
        
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
        if(documentId == null || documentId.isEmpty())
        {
            return;
        }
        
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
        if(documentId == null || documentId.isEmpty())
        {
            return null;
        }
        
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
        
        long version = (long)-1;
        if(solrDocument.getFirstValue("_version_") != null)
        {
            version = Long.parseLong(solrDocument.getFirstValue("_version_").toString());
        }
        
        IndexedDocument newDocument = new IndexedDocument(indexInformations, 
                solrDocument.getFirstValue(UNIQUE_KEY_FIELD).toString(), version);
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
            long version = (long)-1;
            if(document.getFirstValue("_version_") != null)
            {
                version = Long.parseLong(document.getFirstValue("_version_").toString());
            }
            
            hits.add(new SearchHit(indexInformations, document.getFirstValue(UNIQUE_KEY_FIELD).toString(), 
                    version, Float.parseFloat(document.getFirstValue("score").toString()), data));
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
        
        Map<String, Collection<Object>> data = document.getFieldValuesMap();
        Map<String, Object> res = new HashMap<>();
        Object current = res;
        String[] fieldNames = document.getFieldNames().toArray(new String[document.getFieldNames().size()]);
        Arrays.sort(fieldNames);
        for(String name: fieldNames)
        {
            String[] fieldParts = name.split("\\.");
            if(fieldParts.length == 1)  //simple
            {
                addDataToObject(res, data.get(name), name);
            }
            else if(fieldParts.length > 1)  //nested
            {
                int i = 0;
                Object root = current;
                String lastPart = "";
                for(String partOfField: fieldParts)
                {
                    lastPart = partOfField;
                    
                    if(current instanceof Map && ((Map)current).containsKey(partOfField))
                    {
                        current = ((Map)current).get(partOfField);
                        i++;
                        continue;
                    }
                    
                    try
                    {
                        int index = Integer.parseInt(partOfField);  //field part is List

                        List<Object> c = (List)current;
                        if(c.size() > index)    //index already exists
                        {
                            current = c.get(index);
                        } 
                        else if(c.size() == index)  //next in order
                        {
                            Object nextObject = getNextObject(fieldParts, i);
                            if(nextObject != null)  //not last index
                            {
                                c.add(nextObject);
                                current = nextObject;
                            }
                        }
                        else    //skip indexes
                        {
                            while(c.size() != index)    //skip until
                            {
                                c.add(null);
                            }
                            
                            Object nextObject = getNextObject(fieldParts, i);
                            if(nextObject != null)  //not last index
                            {
                                c.add(nextObject);
                                current = nextObject;
                            }
                        }
                    }
                    catch(NumberFormatException ex) //field part is Map
                    {
                        Object nextObject = getNextObject(fieldParts, i);
                        if(nextObject != null)  //not last index
                        {
                            ((Map)current).put(partOfField, nextObject);
                            current = nextObject;
                        }
                    }
                    i++;
                }
                addDataToObject(current, data.get(name), lastPart);
                current = root;
            }
        }
        
        //remove no data fieldParts
        res.remove("_version_");
        res.remove(UNIQUE_KEY_FIELD);
        res.remove("score");
        
        return res;
    }
    
    private Object getNextObject(String[] fieldParts, int i)
    {
        if(fieldParts.length == i+1)    //last part
        {
            return null;
        }
        else
        {
            String nextPart = fieldParts[i+1];
            try
            {
                Integer.parseInt(nextPart);
                return new ArrayList<>();
            }
            catch(NumberFormatException ex)
            {
                return new HashMap<>();
            }
        }
    }
    
    private void addDataToObject(Object o, Collection<Object> data, String index)
    {
        if(o instanceof List)
        {
            List<Object> tmp = (List)o;
            if(data.size() == 1)
            {
                tmp.add(data.toArray()[0]);    //single value
            }
            else
            {
                List<Object> l = new ArrayList<>();
                for(Object value: data)
                {
                    l.add(value);
                }
                tmp.add(l);
            }
        }
        else if(o instanceof Map)
        {
            Map<String, Object> tmp = (Map)o;
            if(data.size() == 1)
            {
                tmp.put(index, data.toArray()[0]);    //single value
            }
            else
            {
                List<Object> l = new ArrayList<>();
                for(Object value: data)
                {
                    l.add(value);
                }
                tmp.put(index, l);
            }
        }
    }
    
    private String[] getAllFieldsWithOptionOrAll(String option)
    {
        if(allFieldsWithOptionOrAll.containsKey(option))
        {
            return allFieldsWithOptionOrAll.get(option);
        }
        
        List<String> all = new ArrayList<>();
        List<String> res = new ArrayList<>();
        SolrQuery q = new SolrQuery();
        q.setRequestHandler("/schema");
        QueryResponse query;
        try 
        {
            query = client.query(q);
            NamedList<Object> schema = (NamedList<Object>)query.getResponse().get("schema");
            List<NamedList<Object>> fields =(List<NamedList<Object>>)schema.get("fields");
      
            for(NamedList<Object> field: fields)
            {
                Object o = field.get(option);
                if(o != null && "true".equals(o.toString().toLowerCase()))
                {
                    res.add(field.get("name").toString());
                }
                all.add(field.get("name").toString());
            }
            
            if(res.isEmpty())
            {
                allFieldsWithOptionOrAll.put(option, all.toArray(new String[all.size()]));
            }
            else
            {
                allFieldsWithOptionOrAll.put(option, res.toArray(new String[res.size()]));
            }
        } 
        catch (SolrServerException | IOException | NullPointerException ex) 
        {
            LOGGER.warn("Cannot read Solr schema because: "+ex.getMessage());
        }
        
        return allFieldsWithOptionOrAll.get(option);
    }
    
    private SolrInputDocument toSolrInputDocument(Map<String, Object> data)
    {
        if(data == null || data.isEmpty())
        {
            return null;
        }
        
        SolrInputDocument document = new SolrInputDocument();
 
        for(Map.Entry<String, Object> d: data.entrySet())
        {
            Object value = d.getValue();
            if(value instanceof Map)
            {
                document = toSolrInputDocument((Map)value, document, new StringBuilder(d.getKey()));
            }
            else if(value instanceof List)
            {
                document = toSolrInputDocument((List)value, document, new StringBuilder(d.getKey()));
            }
            else
            {     
                document.addField(d.getKey(), value);
            }
        }
        
        return document;
    }
    
    private SolrInputDocument toSolrInputDocument(Map<String, Object> data, SolrInputDocument document, StringBuilder key)
    {
        if(data == null)
        {
            return document;
        }
        
        int end = key.length();
        for(Map.Entry<String, Object> d: data.entrySet())
        {
            key.append(".").append(d.getKey());
            Object value = d.getValue();
            if(value instanceof Map)
            {
                document = toSolrInputDocument((Map)value, document, key);
            }
            else if(value instanceof List)
            {
                document = toSolrInputDocument((List)value, document, key);
            }
            else
            {     
                document.addField(key.toString(), value);
            }
            key.delete(end, key.length());
        }
        
        return document;
    }
    
    private SolrInputDocument toSolrInputDocument(List<Object> data, SolrInputDocument document, StringBuilder key)
    {
        if(data == null)
        {
            return document;
        }
        
        int end = key.length();
        List<Object> l = new ArrayList();
        for(int i = 0; i<data.size(); i++)
        {
            key.append(".").append(i);
            Object value = data.get(i);
            if(value instanceof Map)
            {
                document = toSolrInputDocument((Map)value, document, key);
            }
            else if(value instanceof List)
            {
                document = toSolrInputDocument((List)value, document, key);
            }
            else
            {     
                l.add(value);                
            }
            key.delete(end, key.length());
        }
        document.addField(key.toString(), l);
        
        return document;
    }
}
   
