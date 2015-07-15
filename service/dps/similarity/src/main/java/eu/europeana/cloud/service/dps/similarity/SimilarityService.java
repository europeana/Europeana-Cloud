package eu.europeana.cloud.service.dps.similarity;

import eu.europeana.cloud.mcs.driver.FileServiceClient;
import eu.europeana.cloud.service.dps.index.IndexFields;
import eu.europeana.cloud.service.dps.index.Indexer;
import eu.europeana.cloud.service.dps.index.IndexerFactory;
import eu.europeana.cloud.service.dps.index.exception.IndexerException;
import eu.europeana.cloud.service.dps.index.structure.IndexerInformations;
import eu.europeana.cloud.service.dps.index.structure.SearchHit;
import eu.europeana.cloud.service.dps.index.structure.SearchResult;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Pavel Kefurt <Pavel.Kefurt@gmail.com>
 */
public class SimilarityService 
{
    private final float duplicationThreshold = (float)0.98; //1 = 100%
    private final Indexer client;
    
    private static final Logger LOGGER = LoggerFactory.getLogger(SimilarityService.class);
    
    private Map<String, List<String>> fields;
    
    public SimilarityService(String indexer, String index, String type, String addresses) throws IndexerException 
    {  
        Indexer tmp = IndexerFactory.getIndexer(new IndexerInformations(indexer, index, type, addresses));
        
        if(tmp == null)
        {
            LOGGER.warn("No indexer.");
            throw new IndexerException("Unsupported indexer.");
        }
        
        client = tmp;
        
        initFields();
    }
    
    public SimilarityService(String indexer, String index, String type, String addresses, 
            Map<String, List<String>> fields) throws IndexerException 
    {  
        Indexer tmp = IndexerFactory.getIndexer(new IndexerInformations(indexer, index, type, addresses));
        
        if(tmp == null)
        {
            LOGGER.warn("No indexer.");
            throw new IndexerException("Unsupported indexer.");
        }
        
        client = tmp;
        
        this.fields = fields;
    }
    
    public SearchResult getSimilarDocuments(String documentId) throws IndexerException
    {    
        if(documentId == null)
        {
            return null;
        } 
        
        List<String> tmp = getFields(documentId);      
        String[] _fields = tmp != null ? tmp.toArray(new String[tmp.size()]) : null;
        
        return client.getMoreLikeThis(documentId, _fields);         
    }
    
    public SearchResult getSimilarDocuments(String documentId, int limit) throws IndexerException
    {      
        if(documentId == null || limit <= 0)
        {
            return null;
        }
        
        List<String> tmp = getFields(documentId);
        String[] _fields = tmp != null ? tmp.toArray(new String[tmp.size()]) : null;
        
        return client.getMoreLikeThis(documentId, _fields, limit, 0);         
    }
    
    public List<SearchHit> calcDuplicateDocuments(String documentId) throws IndexerException
    {
        if(documentId == null)
        {
            return null;
        }
        
        List<String> tmp = getFields(documentId);
        String[] _fields = tmp != null ? tmp.toArray(new String[tmp.size()]) : null;
        
        SearchResult result = client.getMoreLikeThis(documentId, _fields, 
                50, 2, Indexer.MIN_DOC_FREQ, Indexer.MAX_DOC_FREQ, 
                Indexer.MIN_WORD_LENGTH, Indexer.MAX_WORD_LENGTH, Indexer.PAGE_SIZE, 0, true); 
        
        SearchHit reference = null;
        for(SearchHit sh: result.getHits())
        {
            if(documentId.equals(sh.getId()))
            {
                reference = sh;
                
                result.getHits().remove(sh);
                break;
            }
        }
        
        if(reference == null)
        {
            return null;
        }
        
        float threshold = reference.getScore() * duplicationThreshold;
        List<SearchHit> res = new ArrayList();
        for(SearchHit sh: result.getHits())
        {
            if(sh.getScore() >= threshold)
            {
                res.add(sh);
            }
        }
        
        return res;
    }
    
    private List<String> getFields(String url)
    {
        if(fields == null || fields.isEmpty() || url == null)
        {
            return null;
        }
        
        Map<String, String> parsed = FileServiceClient.parseFileUri(url);
        
        String repName = parsed.get("REPRESENTATIONNAME");
        
        return fields.get(repName.toUpperCase());
    }
    
    private void initFields()
    {
        List<String> pdf = new ArrayList();
        pdf.add(IndexFields.RAW_TEXT.toString());
        pdf.add("another_field");
        
        String[] oai = 
        {
            "description",
            "title"
        };
        
        Map<String, List<String>> _fields = new HashMap<>();
        _fields.put("PDF", pdf);
        _fields.put("TXT", null);
        _fields.put("OAI", Arrays.asList(oai));
    }
}
