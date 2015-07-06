package eu.europeana.cloud.service.dps.index;

import eu.europeana.cloud.service.dps.index.exception.IndexerException;
import eu.europeana.cloud.service.dps.index.structure.IndexerInformations;

/**
 *
 * @author Pavel Kefurt <Pavel.Kefurt@gmail.com>
 */
public class IndexerFactory 
{   
    public static Indexer getIndexer(IndexerInformations ii)
    {
        try
        {
            switch(ii.getIndexerName())
            {
                case ELASTICSEARCH_INDEXER:
                    return new Elasticsearch(ii);
                case SOLR_INDEXER:
                    return null;
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
