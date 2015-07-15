package eu.europeana.cloud.service.dps.index;

import eu.europeana.cloud.service.dps.index.exception.ConnectionException;
import eu.europeana.cloud.service.dps.index.exception.IndexerException;
import eu.europeana.cloud.service.dps.index.exception.ParseDataException;
import eu.europeana.cloud.service.dps.index.structure.IndexerInformations;
import eu.europeana.cloud.service.dps.index.structure.IndexedDocument;
import eu.europeana.cloud.service.dps.index.structure.SearchHit;
import eu.europeana.cloud.service.dps.index.structure.SearchResult;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.index.query.MoreLikeThisQueryBuilder;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryStringQueryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 *
 * @author Pavel Kefurt <Pavel.Kefurt@gmail.com>
 */
public class Elasticsearch implements Indexer
{
    private final IndexerInformations indexInformations;
    
    private final Client client;
    
    private final Map<String, Method> advancedSearchMethods;
    
    private static final Logger LOGGER = LoggerFactory.getLogger(Elasticsearch.class);

    public Elasticsearch(IndexerInformations ii) throws IndexerException
    {   
        indexInformations = ii;
        
        TransportClient transportClient = new TransportClient();
        
        for(String address: ii.getAddresses())
        {
            String[] split = address.split(":");
            if(split.length == 2)
            {
                transportClient.addTransportAddress(new InetSocketTransportAddress(split[0], Integer.parseInt(split[1])));
            }
            else
            {
                LOGGER.warn("Can not parse address '{}' because: Bad address format", address);
            }
        }
        
        if(transportClient.transportAddresses().size() > 0)
        {
            client = transportClient;
        }
        else
        {
            throw new IndexerException("Empty Elasticsearch claster address list.");
        }
        
        
        advancedSearchMethods = new HashMap<>();      
        try 
        {           
            advancedSearchMethods.put("default_field", QueryStringQueryBuilder.class.getMethod("defaultField", String.class));
            advancedSearchMethods.put("default_operator", QueryStringQueryBuilder.class.getMethod("defaultOperator", QueryStringQueryBuilder.Operator.class));
        } 
        catch (NoSuchMethodException | SecurityException ex) 
        {
            LOGGER.error("Preparation of advanced search faild! Message: {}", ex.getMessage());
        }
    }
    
    public Elasticsearch(String clasterAddresses, String index, String type) throws IndexerException 
    {
        this(new IndexerInformations(SupportedIndexers.ELASTICSEARCH_INDEXER.name(), index, type, clasterAddresses));
    }

    @Override
    protected void finalize() throws Throwable 
    {
        if(client != null)
        {
            client.close();
        }
        super.finalize();
    }
    
    @Override
    public Object getIndexer() 
    {
        return client;
    }

    @Override
    public SupportedIndexers getIndexerName() 
    {
        return SupportedIndexers.ELASTICSEARCH_INDEXER;
    }

    @Override
    public IndexerInformations getIndexerInformations() 
    {
        return indexInformations;
    }
    
    @Override
    public IndexedDocument getDocument(String documentId) 
            throws ParseDataException, ConnectionException, IndexerException
    {
        GetResponse response;
        try
        {
            response = client.prepareGet()
                    .setIndex(indexInformations.getIndex())
                    .setType(indexInformations.getType())
                    .setId(documentId)
                    .execute()
                    .actionGet();
        }
        catch(MapperParsingException ex)
        {
            throw new ParseDataException(ex);
        }
        catch(NoNodeAvailableException ex)
        {
            throw new ConnectionException(ex);
        }
        catch(ElasticsearchException ex)
        {
            throw new IndexerException(ex);
        }
        
        if(response.isExists())
        {
            IndexedDocument document = new IndexedDocument(indexInformations, response.getId(), response.getVersion());
            if(!response.isSourceEmpty())
            {
                document.setData(response.getSource());
            }
            
            return document;
        }
        
        return null;
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
    public SearchResult getMoreLikeThis(String documentId, String[] fields, int size, int timeout) throws ConnectionException, IndexerException
    {
        return getMoreLikeThis(documentId, fields, MAX_QUERY_TERMS, MIN_TERM_FREQ, MIN_DOC_FREQ, 
                MAX_DOC_FREQ, MIN_WORD_LENGTH, MAX_WORD_LENGTH, size, timeout, false);
    }
    
    @Override
    public SearchResult getMoreLikeThis(String documentId, String[] fields, int maxQueryTerms, int minTermFreq, 
            int minDocFreq, int maxDocFreq, int minWordLength, int maxWordLength, int size, int timeout, Boolean includeItself) 
            throws ConnectionException, IndexerException 
    {
        MoreLikeThisQueryBuilder mlt;
        if(fields != null)
        {
            mlt = new MoreLikeThisQueryBuilder(fields);
        }
        else
        {
            mlt = new MoreLikeThisQueryBuilder();
        }
        
        mlt.ids(documentId)
            .maxQueryTerms(maxQueryTerms)
            .minTermFreq(minTermFreq)              
            .minDocFreq(minDocFreq)
            .maxDocFreq(maxDocFreq)
            .minWordLength(minWordLength)
            .maxWordLength(maxWordLength)
            .include(includeItself);
               
        SearchRequestBuilder request = client.prepareSearch(indexInformations.getIndex())
                .setTypes(indexInformations.getType())
                .setQuery(mlt)
                .setSize(size);
        
        if(timeout > 0)
        {
            request.setScroll(new TimeValue(timeout));
        }
        
        SearchResponse response;
        try
        {
            response= request.execute().actionGet();               
        }
        catch(NoNodeAvailableException ex)
        {
            throw new ConnectionException(ex);
        }
        catch(ElasticsearchException ex)
        {
            throw new IndexerException(ex);
        }
        
        SearchResult res = fromSearchResponseToSearchResult(response);
        res.setQueryType(SearchResult.QueryTypes.MORE_LIKE_THIS);
        res.setQuery(request);
        return res;
    }
    
    @Override
    public SearchResult search(String text, String[] fields) throws ConnectionException, IndexerException
    {
        return search(text, fields, PAGE_SIZE, 0);
    }
    
    @Override
    public SearchResult search(String text, String[] fields, int size, int timeout) throws ConnectionException, IndexerException
    {
        SearchRequestBuilder request = client.prepareSearch(indexInformations.getIndex())
                .setTypes(indexInformations.getType())
                .setQuery(QueryBuilders.multiMatchQuery(text, fields))
                .setSize(size);
        
        if(timeout > 0)
        {
            request.setScroll(new TimeValue(timeout));
        }
        
        SearchResponse response;
        try
        {
            response = request.execute().actionGet();
        }
        catch(NoNodeAvailableException ex)
        {
            throw new ConnectionException(ex);
        }
        catch(ElasticsearchException ex)
        {
            throw new IndexerException(ex);
        }
        
        SearchResult res = fromSearchResponseToSearchResult(response);
        res.setQueryType(SearchResult.QueryTypes.SEARCH);
        res.setQuery(request);
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
    public SearchResult searchPhraseInFullText(String text, int proximity) 
            throws ConnectionException, IndexerException 
    {
        return searchPhrase(text, IndexFields.RAW_TEXT.toString(), proximity, PAGE_SIZE, 0);
    }
    
    @Override
    public SearchResult searchPhrase(String text, String field, int proximity) 
            throws ConnectionException, IndexerException 
    {
        return searchPhrase(text, field, proximity, PAGE_SIZE, 0);
    }

    @Override
    public SearchResult searchPhrase(String text, String field, int proximity, int size, int timeout) 
            throws ConnectionException, IndexerException 
    {
        SearchRequestBuilder request = client.prepareSearch(indexInformations.getIndex())
                .setTypes(indexInformations.getType())
                .setQuery(QueryBuilders.matchPhraseQuery(field, text).slop(proximity))
                .setSize(size);
        
        if(timeout > 0)
        {
            request.setScroll(new TimeValue(timeout));
        }
        
        SearchResponse response;
        try
        {
            response = request.execute().actionGet();
        }
        catch(NoNodeAvailableException ex)
        {
            throw new ConnectionException(ex);
        }
        catch(ElasticsearchException ex)
        {
            throw new IndexerException(ex);
        }
        
        SearchResult res = fromSearchResponseToSearchResult(response);
        res.setQueryType(SearchResult.QueryTypes.SEARCH);
        res.setQuery(request);
        return res;
    }
    
    @Override
    public SearchResult advancedSearch(String query) throws ConnectionException, IndexerException 
    {
        return advancedSearch(query, null, PAGE_SIZE, 0);
    }

    @Override
    public SearchResult advancedSearch(String query, int size, int timeout) throws ConnectionException, IndexerException 
    {
        return advancedSearch(query, null, size, timeout);
    }

    @Override
    public SearchResult advancedSearch(String query, Map<String, Object> parameters) throws ConnectionException, IndexerException 
    {
        return advancedSearch(query, parameters, PAGE_SIZE, 0);
    }

    @Override
    public SearchResult advancedSearch(String query, Map<String, Object> parameters, int size, int timeout) 
            throws ConnectionException, IndexerException 
    {
        QueryStringQueryBuilder builder = new QueryStringQueryBuilder(query);
        
        if(parameters != null)
        {
            for(Map.Entry<String, Object> parameter: parameters.entrySet())
            {
                Method m = advancedSearchMethods.get(parameter.getKey());
                if(m == null)
                {
                    LOGGER.warn("Unknown parameter {}", parameter.getKey());
                    continue;
                }
                
                try
                {
                    m.invoke(builder, parameter.getValue());
                }
                catch(Exception ex)
                {
                    LOGGER.warn("Search parameter {} is not set because: {}", parameter.getKey(), ex.getMessage());
                }
            }
        }
        
        SearchRequestBuilder request = client.prepareSearch(indexInformations.getIndex())
                .setTypes(indexInformations.getType())
                .setQuery(builder)
                .setSize(size);
        
        if(timeout > 0)
        {
            request.setScroll(new TimeValue(timeout));
        }
        
        SearchResponse response;
        try//TODO: parser exception???!
        {
            response= request.execute().actionGet();               
        }
        catch(NoNodeAvailableException ex)
        {
            throw new ConnectionException(ex);
        }
        catch(ElasticsearchException ex)
        {
            throw new IndexerException(ex);
        }
        
        SearchResult res = fromSearchResponseToSearchResult(response);
        res.setQueryType(SearchResult.QueryTypes.SEARCH);
        res.setQuery(request);
        return res;
    }

    @Override
    public void insert(String data) throws ParseDataException, ConnectionException, IndexerException
    {
        try
        {
            client.prepareIndex()
                    .setIndex(indexInformations.getIndex())
                    .setType(indexInformations.getType())
                    .setSource(data)
                    .execute();
        }
        catch(MapperParsingException ex)
        {
            throw new ParseDataException(ex);
        }
        catch(NoNodeAvailableException ex)
        {
            throw new ConnectionException(ex);
        }
        catch(ElasticsearchException ex)
        {
            throw new IndexerException(ex);
        }
    }
    
    @Override
    public void insert(Map<String, Object> data) throws ParseDataException, ConnectionException, IndexerException
    {
        try
        {
            client.prepareIndex()
                    .setIndex(indexInformations.getIndex())
                    .setType(indexInformations.getType())
                    .setSource(data)
                    .execute();
        }
        catch(MapperParsingException ex)
        {
            throw new ParseDataException(ex);
        }
        catch(NoNodeAvailableException ex)
        {
            throw new ConnectionException(ex);
        }
        catch(ElasticsearchException ex)
        {
            throw new IndexerException(ex);
        }
    }
    
    @Override
    public void insert(String documentId, String data) throws ParseDataException, ConnectionException, IndexerException
    {
        try
        {
            client.prepareIndex()
                    .setIndex(indexInformations.getIndex())
                    .setType(indexInformations.getType())
                    .setId(documentId)
                    .setSource(data)
                    .execute();
        }
        catch(MapperParsingException ex)
        {
            throw new ParseDataException(ex);
        }
        catch(NoNodeAvailableException ex)
        {
            throw new ConnectionException(ex);
        }
        catch(ElasticsearchException ex)
        {
            throw new IndexerException(ex);
        }
    }
    
    @Override
    public void insert(String documentId, Map<String, Object> data) 
            throws ParseDataException, ConnectionException, IndexerException
    {
        try
        {
            client.prepareIndex()
                    .setIndex(indexInformations.getIndex())
                    .setType(indexInformations.getType())
                    .setId(documentId)
                    .setSource(data)
                    .execute();
        }
        catch(MapperParsingException ex)
        {
            throw new ParseDataException(ex);
        }
        catch(NoNodeAvailableException ex)
        {
            throw new ConnectionException(ex);
        }
        catch(ElasticsearchException ex)
        {
            throw new IndexerException(ex);
        }
    }
    
    @Override
    public void update(String documentId, String data) throws ParseDataException, ConnectionException, IndexerException
    {
        try
        {
            client.prepareUpdate()
                    .setIndex(indexInformations.getIndex())
                    .setType(indexInformations.getType())
                    .setId(documentId)
                    .setDocAsUpsert(true)
                    .setDoc(data)
                    .execute();
        }
        catch(ElasticsearchParseException ex)
        {
            throw new ParseDataException(ex);
        }
        catch(NoNodeAvailableException ex)
        {
            throw new ConnectionException(ex);
        }
        catch(ElasticsearchException ex)
        {
            throw new IndexerException(ex);
        }
    }
    
    @Override
    public void update(String documentId, Map<String, Object> data) throws ParseDataException, ConnectionException, IndexerException
    {
        try
        {
            client.prepareUpdate()
                    .setIndex(indexInformations.getIndex())
                    .setType(indexInformations.getType())
                    .setId(documentId)
                    .setDocAsUpsert(true)
                    .setDoc(data)
                    .execute();
        }
        catch(ElasticsearchParseException ex)
        {
            throw new ParseDataException(ex);
        }
        catch(NoNodeAvailableException ex)
        {
            throw new ConnectionException(ex);
        }
        catch(ElasticsearchException ex)
        {
            throw new IndexerException(ex);
        }
    }
    
    @Override
    public void delete(String documentId) throws ConnectionException, IndexerException
    {
        try
        {
            client.prepareDelete()
                    .setIndex(indexInformations.getIndex())
                    .setType(indexInformations.getType())
                    .setId(documentId)
                    .execute();
        }
        catch(NoNodeAvailableException ex)
        {
            throw new ConnectionException(ex);
        }
        catch(ElasticsearchException ex)
        {
            throw new IndexerException(ex);
        }
    }

    @Override
    public SearchResult getNextPage(String scrollId, Object context) throws ConnectionException, IndexerException 
    {
        if(scrollId == null)
        {
            return null;
        }
        
        SearchResult.QueryTypes qType = SearchResult.QueryTypes.UNKNOWN;
        
        SearchScrollRequestBuilder builder = client.prepareSearchScroll(scrollId);
        
        if(context != null)
        {
            if(context instanceof SearchResult)
            {
                SearchResult sr = (SearchResult)context;
                
                qType = sr.getQueryType();
                
                SearchRequestBuilder srb = (SearchRequestBuilder) sr.getQuery();

                builder.setScroll(srb.request().scroll().keepAlive());             
            }
            else if(context instanceof SearchRequestBuilder)
            {
                SearchRequestBuilder srb = (SearchRequestBuilder) context;

                builder.setScroll(srb.request().scroll().keepAlive());
            }
            else if(context instanceof Long)
            {
                builder.setScroll(new TimeValue((Long)context));
            }
        }      
        //if scroll is not set than scroll will not continue
        
        SearchResponse response;
        try
        {
            response = builder.execute().actionGet();
        }
        catch(NoNodeAvailableException ex)
        {
            throw new ConnectionException(ex);
        }
        catch(ElasticsearchException ex)
        {
            throw new IndexerException(ex);
        }

        SearchResult res = fromSearchResponseToSearchResult(response);
        
        if(res.getHits().isEmpty())
        {
            //scroll is ended
            return null;
        }
        
        res.setQueryType(qType);
        res.setQuery(builder);
        return res;
    }
    
    private SearchResult fromSearchResponseToSearchResult(SearchResponse response)
    {
        if(response == null)
        {
            return new SearchResult(null, 0, 0, -1);
        }
        
        List<SearchHit> hits = new ArrayList();
        for(org.elasticsearch.search.SearchHit hit: response.getHits())
        {
            hits.add(new SearchHit(indexInformations, hit.getId(), hit.getVersion(), hit.getScore(), hit.getSource()));
        }
        
        SearchResult res = new SearchResult(hits, response.getHits().getTotalHits(), response.getHits().getMaxScore(), 
                response.getTookInMillis(), response.getScrollId());
        
        return res;
    }
}
