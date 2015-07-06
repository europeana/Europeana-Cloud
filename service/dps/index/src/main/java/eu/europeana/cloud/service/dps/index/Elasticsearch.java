package eu.europeana.cloud.service.dps.index;

import eu.europeana.cloud.service.dps.index.exception.ConnectionException;
import eu.europeana.cloud.service.dps.index.exception.IndexerException;
import eu.europeana.cloud.service.dps.index.exception.ParseDataException;
import eu.europeana.cloud.service.dps.index.structure.IndexerInformations;
import eu.europeana.cloud.service.dps.index.structure.IndexedDocument;
import eu.europeana.cloud.service.dps.index.structure.SearchHit;
import eu.europeana.cloud.service.dps.index.structure.SearchResult;
import java.util.ArrayList;
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
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
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
    
    private static final Logger LOGGER = LoggerFactory.getLogger(Elasticsearch.class);
    
    private static final int PAGE_SIZE = 10;
    
    private static final int MAX_QUERY_TERMS = 1;
    private static final int MIN_TERM_FREQ = 1;
    private static final int MIN_DOC_FREQ = 1;
    private static final int MAX_DOC_FREQ = 1;
    private static final int MIN_WORD_LENGTH = 1;
    private static final int MAX_WORD_LENGTH = 1;

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
            response = client.prepareGet(indexInformations.getIndex(), indexInformations.getType(), documentId)
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
        
        IndexedDocument document = new IndexedDocument(indexInformations, response.getId(), response.getVersion());
        document.setData(response.getSource());
        
        return document;
    }
    
    @Override
    public SearchResult getMoreLikeThis(String documentId) throws ConnectionException, IndexerException
    {
        String[] fields = {"_all"};
        return getMoreLikeThis(documentId, fields, MAX_QUERY_TERMS, MIN_TERM_FREQ, MIN_DOC_FREQ, 
                MAX_DOC_FREQ, MIN_WORD_LENGTH, MAX_WORD_LENGTH, PAGE_SIZE, 0);
    }
    
    @Override
    public SearchResult getMoreLikeThis(String documentId, int size, int timeout) throws ConnectionException, IndexerException
    {
        String[] fields = {"_all"};
        return getMoreLikeThis(documentId, fields, MAX_QUERY_TERMS, MIN_TERM_FREQ, MIN_DOC_FREQ, 
                MAX_DOC_FREQ, MIN_WORD_LENGTH, MAX_WORD_LENGTH, size, timeout);
    }
    
    @Override
    public SearchResult getMoreLikeThis(String documentId, String[] fields) throws ConnectionException, IndexerException
    {
        return getMoreLikeThis(documentId, fields, MAX_QUERY_TERMS, MIN_TERM_FREQ, MIN_DOC_FREQ, 
                MAX_DOC_FREQ, MIN_WORD_LENGTH, MAX_WORD_LENGTH, PAGE_SIZE, 0);
    }
    
    @Override
    public SearchResult getMoreLikeThis(String documentId, String[] fields, int size, int timeout) throws ConnectionException, IndexerException
    {
        return getMoreLikeThis(documentId, fields, MAX_QUERY_TERMS, MIN_TERM_FREQ, MIN_DOC_FREQ, 
                MAX_DOC_FREQ, MIN_WORD_LENGTH, MAX_WORD_LENGTH, size, timeout);
    }
    
    @Override
    public SearchResult getMoreLikeThis(String documentId, String[] fields, int maxQueryTerms, int minTermFreq, 
            int minDocFreq, int maxDocFreq, int minWordLength, int maxWordLength, int size, int timeout) 
            throws ConnectionException, IndexerException 
    {
       
        QueryBuilder mlt = new MoreLikeThisQueryBuilder(fields)              
                .ids(documentId)
                .maxQueryTerms(maxQueryTerms)
                .minTermFreq(minTermFreq)              
                .minDocFreq(minDocFreq)
                .maxDocFreq(maxDocFreq)
                .minWordLength(minWordLength)
                .maxWordLength(maxWordLength);
                
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
        
        List<SearchHit> hits = new ArrayList();
        for(org.elasticsearch.search.SearchHit hit: response.getHits())
        {
            hits.add(new SearchHit(indexInformations, hit.getId(), hit.getVersion(), hit.getScore(), hit.getSource()));
        }
        
        SearchResult res = new SearchResult(hits, response.getHits().getTotalHits(), response.getHits().getMaxScore(), 
                response.getTookInMillis(), response.getScrollId());
        res.setQueryType(SearchResult.QueryTypes.MORE_LIKE_THIS);
        res.setQuery(request);
        return res;
    }
    
    @Override
    public SearchResult search(String text, IndexFields[] fields) throws ConnectionException, IndexerException
    {
        List<String> newFields = new ArrayList<>();
        for(IndexFields f: fields)
        {
            newFields.add(f.toString());
        }
        
        return search(text, (String[]) newFields.toArray(), PAGE_SIZE, 0);
    }
    
    @Override
    public SearchResult search(String text, IndexFields[] fields, int size, int timeout) throws ConnectionException, IndexerException
    {
        List<String> newFields = new ArrayList<>();
        for(IndexFields f: fields)
        {
            newFields.add(f.toString());
        }
        
        return search(text, (String[]) newFields.toArray(), size, timeout);
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
        
        List<SearchHit> hits = new ArrayList<>();
        for(org.elasticsearch.search.SearchHit hit: response.getHits())
        {
            hits.add(new SearchHit(indexInformations, hit.getId(), hit.getVersion(), hit.getScore(), hit.getSource()));
        }
        
        SearchResult res = new SearchResult(hits, response.getHits().getTotalHits(), response.getHits().getMaxScore(), 
                response.getTookInMillis(), response.getScrollId());
        res.setQueryType(SearchResult.QueryTypes.SEARCH);
        res.setQuery(request);
        return res;
    }

    @Override
    public SearchResult searchFullText(String text) throws ConnectionException, IndexerException
    {
        return searchFullText(text, PAGE_SIZE, 0);
    }
    
    @Override
    public SearchResult searchFullText(String text, int size, int timeout) throws ConnectionException, IndexerException
    {
        SearchRequestBuilder request = client.prepareSearch(indexInformations.getIndex())
                .setTypes(indexInformations.getType())
                .setQuery(QueryBuilders.matchQuery(IndexFields.RAW_TEXT.toString(), text))
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
        
        List<SearchHit> hits = new ArrayList<>();
        for(org.elasticsearch.search.SearchHit hit: response.getHits())
        {
            hits.add(new SearchHit(indexInformations, hit.getId(), hit.getVersion(), hit.getScore(), hit.getSource()));
        }
        
        SearchResult res = new SearchResult(hits, response.getHits().getTotalHits(), response.getHits().getMaxScore(), 
                response.getTookInMillis(), response.getScrollId());   
        res.setQueryType(SearchResult.QueryTypes.SEARCH);
        res.setQuery(request);
        return res;
    }

    @Override
    public void insert(String data) throws ParseDataException, ConnectionException, IndexerException
    {
        try
        {
            client.prepareIndex(indexInformations.getIndex(), indexInformations.getType())
                    .setSource(data)
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
    }
    
    @Override
    public void insert(Map<String, Object> data) throws ParseDataException, ConnectionException, IndexerException
    {
        try
        {
            client.prepareIndex(indexInformations.getIndex(), indexInformations.getType())
                    .setSource(data)
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
    }
    
    @Override
    public void insert(String documentId, String data) throws ParseDataException, ConnectionException, IndexerException
    {
        try
        {
            client.prepareIndex(indexInformations.getIndex(), indexInformations.getType(), documentId)
                    .setSource(data)
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
    }
    
    @Override
    public void insert(String documentId, Map<String, Object> data) 
            throws ParseDataException, ConnectionException, IndexerException
    {
        try
        {
            client.prepareIndex(indexInformations.getIndex(), indexInformations.getType(), documentId)
                    .setSource(data)
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
    }
    
    @Override
    public void update(String documentId, String data) throws ParseDataException, ConnectionException, IndexerException
    {
        try
        {
            client.prepareUpdate(indexInformations.getIndex(), indexInformations.getType(), documentId)
                    .setDocAsUpsert(true)
                    .setDoc(data)
                    .execute()
                    .actionGet();
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
            client.prepareUpdate(indexInformations.getIndex(), indexInformations.getType(), documentId)
                    .setDocAsUpsert(true)
                    .setDoc(data)
                    .execute()
                    .actionGet();
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
        
        List<SearchHit> hits = new ArrayList<>();
        for(org.elasticsearch.search.SearchHit hit: response.getHits())
        {
            hits.add(new SearchHit(indexInformations, hit.getId(), hit.getVersion(), hit.getScore(), hit.getSource()));
        }
        
        if(hits.isEmpty())
        {
            //scroll is ended
            return null;
        }
        
        SearchResult res = new SearchResult(hits, response.getHits().getTotalHits(), response.getHits().getMaxScore(), 
                response.getTookInMillis(), response.getScrollId());
        res.setQueryType(qType);
        res.setQuery(builder);
        return res;
    }
}
