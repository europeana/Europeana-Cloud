package eu.europeana.cloud.service.mcs.persistent.exception;

/**
 * Exception to be thrown if there where no documents in the Solr index matching requested query.
 */
public class SolrDocumentNotFoundException extends Exception {

    /**
     * Constructs a SolrDocumentNotFoundException with the specified Solr query.
     * 
     * @param query
     *            the Solr query
     */
    public SolrDocumentNotFoundException(String query) {
        super(String.format("Solr document not found for query %s", query));
    }
}
