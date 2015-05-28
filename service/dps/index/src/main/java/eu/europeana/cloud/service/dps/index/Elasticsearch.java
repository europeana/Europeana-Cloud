package eu.europeana.cloud.service.dps.index;

import eu.europeana.cloud.service.dps.index.exception.IndexException;
import eu.europeana.cloud.service.dps.index.structure.IndexedDocument;
import eu.europeana.cloud.service.dps.index.structure.SearchResult;
import eu.europeana.cloud.service.dps.index.structure.TfIdf;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.QueryBuilder;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.termvector.TermVectorResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 *
 * @author Pavel Kefurt <Pavel.Kefurt@gmail.com>
 */
public class Elasticsearch
{
    private final String index;
    private final String type;
    
    private final Client client;
    
    private static final Logger LOGGER = LoggerFactory.getLogger(Elasticsearch.class);

    public Elasticsearch(String clasterAddresses, String index, String type) throws IndexException 
    {
        this.index = index;
        this.type = type;
        
        String[] addresses = clasterAddresses.split(";");
        
        TransportClient transportClient = new TransportClient();
        
        for(String address: addresses)
        {
            if(address != null && !address.isEmpty())
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
        }
        
        if(transportClient.transportAddresses().size() > 0)
        {
            client = transportClient;
        }
        else
        {
            throw new IndexException("Empty Elasticsearch claster address list.");
        }
    }

    @Override
    protected void finalize() throws Throwable 
    {
        client.close();
        super.finalize();
    }
    
    public IndexedDocument getDocument(String documentId)
    {
        GetResponse response = client.prepareGet(index, type, documentId).execute().actionGet();
        
        IndexedDocument document = new IndexedDocument(index, type, response.getId(), response.getVersion());
        document.setData(response.getSource());
        
        return document;
    }
    
    public Map<String, Map<String, TfIdf>> getTfIdfVectors(String documentId, String... fields)
    {
        //TODO: if field is not set or with wrong mapping
        
        TermVectorResponse termVector = client.prepareTermVector(index, type, documentId)
                .setTermStatistics(true)
                .setFieldStatistics(false)
                .setSelectedFields(fields)
                .setPositions(false)
                .setOffsets(false)
                .setPayloads(false)
                .execute()
                .actionGet();
        
        CountResponse numDocuments = client.prepareCount(index, type)
                .execute()
                .actionGet();
        
        Map<String, Map<String, TfIdf>> req = new HashMap<>();
        try 
        {
            Fields _fields = termVector.getFields();
            for(String field: _fields)
            {
                Terms terms = _fields.terms(field);
                TermsEnum termsEnum = terms.iterator(TermsEnum.EMPTY);

                long effectiveDocumentLength = 0;   //sum of term frequencies
                
                Map<String, TfIdf> tfIdf_vector = new HashMap<>();
                while (termsEnum.next() != null)                  
                {                                   
                    //term frequency in the field
                    int termFreq = termsEnum.docs(null, null, DocsEnum.FLAG_FREQS).freq();  //lucene version 4
                    //int termFreq = termsEnum.postings(null, null, PostingsEnum.FREQS).freq(); //lucene version 5
                    
                    effectiveDocumentLength += termFreq;
                    
                    TfIdf ftIdf = new TfIdf();
                    ftIdf.setTermFrequency(termFreq);
                    ftIdf.setDocumentFrequency(termsEnum.docFreq());
                    
                    tfIdf_vector.put(termsEnum.term().utf8ToString(), ftIdf);
                }
                
                long numberOfDocuments = numDocuments.getCount();
                for(Map.Entry<String, TfIdf> item: tfIdf_vector.entrySet())
                {
                    TfIdf tfIdf = item.getValue();
                    tfIdf.setNumberOfDocuments(numberOfDocuments);
                    tfIdf.setDocumentLength(effectiveDocumentLength);
                }
                
                req.put(field, tfIdf_vector);
            }
        } 
        catch (IOException ex) 
        {
            LOGGER.warn("Can not create TF-IDF vectors because: {}", ex.getMessage());
        } 
        
        return req;
    }
    
    public List<SearchResult> simpleMatchQuery(String field, String text, int numberOfResults)
    {
        SearchResponse response = client.prepareSearch(index)
                .setTypes(type)
                .setQuery(QueryBuilders.matchQuery(field, text))
                .setSize(numberOfResults)
                .execute()
                .actionGet();
        
        List<SearchResult> ret = new ArrayList();
        for(SearchHit hit: response.getHits())
        {
            ret.add(new SearchResult(index, type, hit.getId(), hit.getVersion(), hit.getScore(), hit.getSource()));
        }
        
        return ret;
    }
    
    public List<SearchResult> simpleMatchQuery(String field, String text, float minScore)
    {
        SearchResponse response = client.prepareSearch(index)
                .setTypes(type)
                .setQuery(QueryBuilders.matchQuery(field, text))
                .setMinScore(minScore)
                .setSize(Integer.MAX_VALUE)
                .execute()
                .actionGet();
        
        List<SearchResult> ret = new ArrayList();
        for(SearchHit hit: response.getHits())
        {
            ret.add(new SearchResult(index, type, hit.getId(), hit.getVersion(), hit.getScore(), hit.getSource()));
        }
        
        return ret;
    }
}
