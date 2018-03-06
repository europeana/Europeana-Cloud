package eu.europeana.cloud.service.dps.index;

import eu.europeana.cloud.service.dps.index.exception.IndexerException;
import eu.europeana.cloud.service.dps.index.structure.IndexedDocument;
import eu.europeana.cloud.service.dps.index.structure.IndexerInformations;
import eu.europeana.cloud.service.dps.index.structure.SearchResult;

import java.util.Map;

/**
 * Interface for indexers.
 * @author Pavel Kefurt <Pavel.Kefurt@gmail.com>
 */
public interface Indexer 
{   
    //default values
    public static final int PAGE_SIZE = 10;   
    public static final int TIMEOUT = 0;
    public static final int MAX_QUERY_TERMS = -1;
    public static final int MIN_TERM_FREQ = -1;
    public static final int MIN_DOC_FREQ = -1;
    public static final int MAX_DOC_FREQ = -1;
    public static final int MIN_WORD_LENGTH = -1;
    public static final int MAX_WORD_LENGTH = -1;   
    public static final Boolean INCLUDE_ITSELF = false;
    
    public enum Operator
    {
        AND,
        OR
    }
    
    /**
     * Retrieve indexer.
     * @return instance of client that is used for indexing
     */
    public Object getIndexer();
    
    /**
     * Retrieve name of indexer.
     * @return value from SupportedIndexers
     */
    public SupportedIndexers getIndexerName();
    
    /**
     * Retrieve informations about indexer.
     * @return instance of IndexerInformations
     */
    public IndexerInformations getIndexerInformations();
    
    /**
     * Retrieve documents with similar content. 
     * @param documentId index of reference document
     * @return instance of SearchResult
     * @throws IndexerException
     */
    public SearchResult getMoreLikeThis(String documentId) throws IndexerException;
    
    /**
     * Retrieve documents with similar content. 
     * @param documentId index of reference document
     * @param size number of results on one page
     * @param timeout tells how long it should keep the search context alive. (ms)
     * @return instance of SearchResult
     * @throws IndexerException
     */
    public SearchResult getMoreLikeThis(String documentId, int size, int timeout) throws IndexerException;
    
    /**
     * Retrieve documents with similar content. 
     * @param documentId index of reference document
     * @param fields array of fields to search
     * @return instance of SearchResult
     * @throws IndexerException
     */
    public SearchResult getMoreLikeThis(String documentId, String[] fields) throws IndexerException;
    
    /**
     * Retrieve documents with similar content. 
     * @param documentId index of reference document
     * @param fields array of fields to search
     * @param size number of results on one page
     * @param timeout tells how long it should keep the search context alive. (ms)
     * @return instance of SearchResult
     * @throws IndexerException
     */
    public SearchResult getMoreLikeThis(String documentId, String[] fields, int size, int timeout) throws IndexerException;
    
    /**
     * Retrieve documents with similar content. 
     * @param documentId index of reference document
     * @param fields array of fields to search
     * @param maxQueryTerms maximum number of query terms that will be selected
     * @param minTermFreq minimum term frequency below which the terms will be ignored from the input document
     * @param minDocFreq minimum document frequency below which the terms will be ignored from the input document
     * @param maxDocFreq maximum document frequency above which the terms will be ignored from the input document
     * @param minWordLength minimum word length frequency below which the terms will be ignored
     * @param maxWordLength maximum word length frequency above which the terms will be ignored
     * @param size number of results on one page
     * @param timeout tells how long it should keep the search context alive. (ms)
     * @param includeItself specifies whether the input document should also be included in the search result
     * @return instance of SearchResult
     * @throws IndexerException
     */
    public SearchResult getMoreLikeThis(String documentId, String[] fields, int maxQueryTerms, int minTermFreq, 
            int minDocFreq, int maxDocFreq, int minWordLength, int maxWordLength, 
            int size, int timeout, Boolean includeItself) throws IndexerException;
      
    /**
     * Retrieve documents which contains search text.
     * @param text search text
     * @param fields array of fields to search
     * @return instance of SearchResult
     * @throws eu.europeana.cloud.service.dps.index.exception.IndexerException 
     */
    public SearchResult search(String text, String[] fields) throws IndexerException;
    
    /**
     * Retrieve documents which contains search text.
     * @param text search text
     * @param fields array of fields to search
     * @param size number of results on one page
     * @param timeout tells how long it should keep the search context alive. (ms)
     * @return instance of SearchResult
     * @throws eu.europeana.cloud.service.dps.index.exception.IndexerException 
     */
    public SearchResult search(String text, String[] fields, int size, int timeout) throws IndexerException;
    
    /**
     * Retrieve documents which contains search text in full text field.
     * @param text search text
     * @return instance of SearchResult
     * @throws eu.europeana.cloud.service.dps.index.exception.IndexerException 
     */
    public SearchResult searchFullText(String text) throws IndexerException;
    
    /**
     * Retrieve documents which contains search text in full text field.
     * @param text search text
     * @param size number of results on one page
     * @param timeout tells how long it should keep the search context alive. (ms)
     * @return instance of SearchResult
     * @throws eu.europeana.cloud.service.dps.index.exception.IndexerException 
     */
    public SearchResult searchFullText(String text, int size, int timeout) throws IndexerException;
        
    /**
     * Retrieve documents which contains phrase text in full text field.
     * @param text search text
     * @param slop number of term position moves (edits)  allowed
     * @return instance of SearchResult
     * @throws eu.europeana.cloud.service.dps.index.exception.IndexerException 
     */
    public SearchResult searchPhraseInFullText(String text, int slop) throws IndexerException;
    
    /**
     * Retrieve documents which contains phrase text.
     * @param text search text
     * @param field field to search
     * @param slop number of term position moves (edits)  allowed
     * @return instance of SearchResult
     * @throws eu.europeana.cloud.service.dps.index.exception.IndexerException 
     */
    public SearchResult searchPhrase(String text, String field, int slop) throws IndexerException;
    
    /**
     * Retrieve documents which contains phrase text.
     * @param text search text
     * @param field field to search
     * @param slop number of term position moves (edits)  allowed
     * @param size number of results on one page
     * @param timeout tells how long it should keep the search context alive. (ms)
     * @return instance of SearchResult
     * @throws eu.europeana.cloud.service.dps.index.exception.IndexerException 
     */
    public SearchResult searchPhrase(String text, String field, int slop, int size, int timeout) throws IndexerException;
    
    /**
     * Search documents by lucene query syntax.
     * @param query lucene query
     * @return instance of SearchResult
     * @throws eu.europeana.cloud.service.dps.index.exception.IndexerException 
     */
    public SearchResult advancedSearch(String query) throws IndexerException;
    
    /**
     * Search documents by lucene query syntax.
     * @param query lucene query
     * @param size number of results on one page
     * @param timeout tells how long it should keep the search context alive. (ms)
     * @return instance of SearchResult
     * @throws eu.europeana.cloud.service.dps.index.exception.IndexerException 
     */
    public SearchResult advancedSearch(String query, int size, int timeout) throws IndexerException;
    
    /**
     * Search documents by lucene query syntax.
     * @param query lucene query
     * @param parameters optional settings
     * <ul>
     * <li>default_field [String]</li>
     * <li>default_operator [Operator]</li>
     * <li>analyzer [String]</li>
     * <li>allow_loading_wildcard [Boolean]</li>
     * <li>lowercase_expanded_terms [Boolean]</li>
     * <li>enable_position_increments [Boolean]</li>
     * <li>fuzzy_prefix_length [Integer]</li>
     * <li>fuzzy_max_expansions [Integer]</li>
     * <li>phrase_slop [Integer]</li>
     * <li>boost [Float]</li>
     * <li>analyze_wildcard [Boolean]</li>
     * <li>auto_generate_phrase_queries [Boolean]</li>
     * <li>max_determinized_states [Integer]</li>
     * <li>lenient [Boolean]</li>
     * <li>timeZone [String]</li>
     * </ul>
     * @return instance of SearchResult
     * @throws eu.europeana.cloud.service.dps.index.exception.IndexerException 
     */
    public SearchResult advancedSearch(String query, Map<String, Object> parameters) throws IndexerException;
    
    /**
     * Search documents by lucene query syntax.
     * @param query lucene query
     * @param parameters optional settings
     * <ul>
     * <li>default_field [String]</li>
     * <li>default_operator [Operator]</li>
     * <li>analyzer [String]</li>
     * <li>allow_loading_wildcard [Boolean]</li>
     * <li>lowercase_expanded_terms [Boolean]</li>
     * <li>enable_position_increments [Boolean]</li>
     * <li>fuzzy_prefix_length [Integer]</li>
     * <li>fuzzy_max_expansions [Integer]</li>
     * <li>phrase_slop [Integer]</li>
     * <li>boost [Float]</li>
     * <li>analyze_wildcard [Boolean]</li>
     * <li>auto_generate_phrase_queries [Boolean]</li>
     * <li>max_determinized_states [Integer]</li>
     * <li>lenient [Boolean]</li>
     * <li>timeZone [String]</li>
     * </ul>
     * @param size number of results on one page
     * @param timeout tells how long it should keep the search context alive. (ms)
     * @return instance of SearchResult
     * @throws eu.europeana.cloud.service.dps.index.exception.IndexerException 
     */
    public SearchResult advancedSearch(String query, Map<String, Object> parameters, int size, int timeout) throws IndexerException;
    
    /**
     * Insert data with generated documentId.
     * @param data data as a JSON string (e.g. {"field1":"value1","field2":"value2"})
     * @throws IndexerException
     */
    public void insert(String data) throws IndexerException;
    
    /**
     * Insert data with generated documentId.
     * @param data data as a Map (key = field, value = data)
     * @throws IndexerException
     */
    public void insert(Map<String, Object> data) throws IndexerException;
    
    /**
     * Insert data.
     * @param documentId unique identifier
     * @param data data as a JSON string (e.g. {"field1":"value1","field2":"value2"})
     * @throws IndexerException
     */
    public void insert(String documentId, String data) throws IndexerException;
    
    /**
     * Insert data.
     * @param documentId unique identifier
     * @param data data as a Map (key = field, value = data)
     * @throws IndexerException
     */
    public void insert(String documentId, Map<String, Object> data) throws IndexerException;
    
    /**
     * Update data in record.
     * @param documentId unique identifier of updated record.
     * @param data data as a JSON string (e.g. {"field1":"value1","field2":"value2"})
     * @throws IndexerException
     */
    public void update(String documentId, String data) throws IndexerException;
    
    /**
     * Update data in record.
     * @param documentId unique identifier of updated record.
     * @param data data as a Map (key = field, value = data)
     * @throws IndexerException
     */
    public void update(String documentId, Map<String, Object> data) throws IndexerException;
    
    /**
     * Delete record with documentId.
     * @param documentId unique identifier of deleted record.
     * @throws IndexerException
     */
    public void delete(String documentId) throws IndexerException;
    
    /**
     * Retrieve document.
     * @param documentId unique identifier of document
     * @return instance of IndexedDocument or null
     * @throws IndexerException
     */
    public IndexedDocument getDocument(String documentId) throws IndexerException;
    
    /**
     * Retrieve next page of results.
     * @param scrollId scroll id for next page
     * @param context last Search Result object (recommended) or other informations about search
     * @return instance of SearchResult or null if no other data
     * @throws IndexerException
     */
    public SearchResult getNextPage(String scrollId, Object context) throws IndexerException;
}
