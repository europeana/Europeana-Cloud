package eu.europeana.cloud.service.dps.similarity;

import eu.europeana.cloud.common.web.ParamConstants;
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
 * Service for search similar and duplicate documents.
 * @author Pavel Kefurt <Pavel.Kefurt@gmail.com>
 */
public class SimilarityService 
{
    private final float duplicationThreshold = (float)0.98; //1 = 100%
    private final Indexer client;
    
    private static final Logger LOGGER = LoggerFactory.getLogger(SimilarityService.class);
    
    private Map<String, List<String>> fields;
    
    /**
     * Construct object with default fields.
     * @param indexer name of indexer ({@link eu.europeana.cloud.service.dps.index.SupportedIndexers SupportedIndexers})
     * @param index index name
     * @param type type name ({@link eu.europeana.cloud.service.dps.index.Solr Solr} indexer ignore this parameter)
     * @param addresses indexer servers addresses (separated by semicolon)
     * @throws IndexerException
     */
    public SimilarityService(String indexer, String index, String type, String addresses) throws IndexerException 
    {  
        this(new IndexerInformations(indexer, index, type, addresses));
    }
    
     /**
     * Construct object with custom fields.
     * @param indexer name of indexer ({@link eu.europeana.cloud.service.dps.index.SupportedIndexers SupportedIndexers})
     * @param index index name
     * @param type type name ({@link eu.europeana.cloud.service.dps.index.Solr Solr} indexer ignore this parameter)
     * @param addresses indexer servers addresses (separated by semicolon)
     * @param fields selected fields for every representation. This fields will be used for search similarity and duplicity
     *     Map structure: <"representation name", List<"field name">> (null means all possible fields)
     * @throws IndexerException
     */
    public SimilarityService(String indexer, String index, String type, String addresses, 
            Map<String, List<String>> fields) throws IndexerException 
    {  
        this(new IndexerInformations(indexer, index, type, addresses), fields);
    }
    
    /**
     * Construct object with default fields.
     * @param ii instance of IndexerInformations
     * @throws IndexerException
     */
    public SimilarityService(IndexerInformations ii) throws IndexerException
    {
        Indexer tmp = IndexerFactory.getIndexer(ii);
        
        if(tmp == null)
        {
            LOGGER.warn("No indexer.");
            throw new IndexerException("Unsupported indexer.");
        }
        
        client = tmp;
        
        initFields();
    }
    
    /**
     * Construct object with custom fields.
     * @param ii instance of IndexerInformations
     * @param fields selected fields for every representation. This fields will be used for search similarity and duplicity
     *     Map structure: <"representation name", List<"field name">> (null means all possible fields)
     * @throws IndexerException
     */
    public SimilarityService(IndexerInformations ii, Map<String, List<String>> fields) throws IndexerException
    {
        Indexer tmp = IndexerFactory.getIndexer(ii);
        
        if(tmp == null)
        {
            LOGGER.warn("No indexer.");
            throw new IndexerException("Unsupported indexer.");
        }
        
        client = tmp;
        
        this.fields = fields;
    }
    
    /**
     * Retrieve similar documents for reference document.
     * @param documentId unique identifier of reference document
     * @return instance of SearchResult or null if documentId is not set
     * @throws IndexerException
     */
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
    
    /**
     * Retrieve similar documents for reference document.
     * @param documentId unique identifier of reference document
     * @param limit maximum number of results
     * @return instance of SearchResult or null if documentId is not set
     * @throws IndexerException
     */
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
    
    /**
     * Retrieve duplicate documents for reference document.
     * @param documentId unique identifier of reference document
     * @return list of SearchHits or null if documentId is not set
     * @throws IndexerException
     */
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
            return new ArrayList();
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
        
        String repName = parsed.get(ParamConstants.P_REPRESENTATIONNAME);
        
        return fields.get(repName.toUpperCase());
    }
    
    /**
     * Fields initializer.
     * Structure: Map<"representation name", List<"field name">> (null means all possible fields)
     */
    protected void initFields()
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
