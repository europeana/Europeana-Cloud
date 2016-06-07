package eu.europeana.cloud.service.dps.index;

import eu.europeana.cloud.service.dps.index.exception.IndexerException;
import eu.europeana.cloud.service.dps.index.structure.IndexerInformations;

/**
 * Factory for select indexer.
 * @author Pavel Kefurt <Pavel.Kefurt@gmail.com>
 */
public class IndexerFactory 
{   

    /**
     * Retrieve instance of indexer based on indexer informations.
     * @param ii indexer informations
     * @return selected indexer or null
     */
    public static Indexer getIndexer(IndexerInformations ii)
    {
        try
        {
            switch(ii.getIndexerName())
            {
                case ELASTICSEARCH_INDEXER:
                    return new Elasticsearch(ii);
                case SOLR_INDEXER:
                    return new Solr(ii);
                case UNSUPPORTED:
                default:
                    return null;
            }
        }
        catch(IndexerException ex)
        {
            return null;
        }
    }
}
