package eu.europeana.cloud.service.dps.index;

import eu.europeana.cloud.service.dps.index.exception.IndexerException;
import eu.europeana.cloud.service.dps.index.structure.IndexedDocument;
import eu.europeana.cloud.service.dps.index.structure.IndexerInformations;
import eu.europeana.cloud.service.dps.index.structure.SearchResult;
import java.util.Map;

/**
 *
 * @author Pavel Kefurt <Pavel.Kefurt@gmail.com>
 */
public interface Indexer 
{   
    public static final int PAGE_SIZE = 10;
    
    public static final int MAX_QUERY_TERMS = -1;
    public static final int MIN_TERM_FREQ = -1;
    public static final int MIN_DOC_FREQ = -1;
    public static final int MAX_DOC_FREQ = -1;
    public static final int MIN_WORD_LENGTH = -1;
    public static final int MAX_WORD_LENGTH = -1;
    
    public Object getIndexer();
    
    public SupportedIndexers getIndexerName();
    
    public IndexerInformations getIndexerInformations();
    
    public SearchResult getMoreLikeThis(String documentId) throws IndexerException;
    
    public SearchResult getMoreLikeThis(String documentId, int size, int timeout) throws IndexerException;
    
    public SearchResult getMoreLikeThis(String documentId, String[] fields) throws IndexerException;
    
    public SearchResult getMoreLikeThis(String documentId, String[] fields, int size, int timeout) throws IndexerException;
    
    public SearchResult getMoreLikeThis(String documentId, String[] fields, int maxQueryTerms, int minTermFreq, 
            int minDocFreq, int maxDocFreq, int minWordLength, int maxWordLength, 
            int size, int timeout, Boolean includeItself) throws IndexerException;
      
    public SearchResult search(String text, String[] fields) throws IndexerException;
    
    public SearchResult search(String text, String[] fields, int size, int timeout) throws IndexerException;
    
    public SearchResult searchFullText(String text) throws IndexerException;
    
    /**
     * 
     * @param text
     * @param size number of results on one page
     * @param timeout tells how long it should keep the search context alive. (ms)
     * @return 
     * @throws eu.europeana.cloud.service.dps.index.exception.IndexerException 
     */
    public SearchResult searchFullText(String text, int size, int timeout) throws IndexerException;
        
    public SearchResult searchPhraseInFullText(String text, int proximity) throws IndexerException;
    
    public SearchResult searchPhrase(String text, String field, int proximity) throws IndexerException;
    
    public SearchResult searchPhrase(String text, String field, int proximity, int size, int timeout) throws IndexerException;
    
    public SearchResult advancedSearch(String query) throws IndexerException;
    
    public SearchResult advancedSearch(String query, int size, int timeout) throws IndexerException;
    
    public SearchResult advancedSearch(String query, Map<String, Object> parameters) throws IndexerException;
    
    public SearchResult advancedSearch(String query, Map<String, Object> parameters, int size, int timeouts) throws IndexerException;
    
    public void insert(String data) throws IndexerException;
    
    public void insert(Map<String, Object> data) throws IndexerException;
    
    public void insert(String documentId, String data) throws IndexerException;
    
    public void insert(String documentId, Map<String, Object> data) throws IndexerException;
    
    public void update(String documentId, String data) throws IndexerException;
    
    public void update(String documentId, Map<String, Object> data) throws IndexerException;
    
    public void delete(String documentId) throws IndexerException;
    
    public IndexedDocument getDocument(String documentId) throws IndexerException;
    
    public SearchResult getNextPage(String scrollId, Object context) throws IndexerException;
}
