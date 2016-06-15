package eu.europeana.cloud.service.dps.index.structure;

import java.util.Map;

/**
 * Object representation of one search hit.
 * @author Pavel Kefurt <Pavel.Kefurt@gmail.com>
 */
public class SearchHit extends IndexedDocument
{
    private final float score;

    /**
     * Construct object.
     * @param ii informations about indexer
     * @param id document id
     * @param version version of document
     * @param score score from search
     * @param data indexed document data
     */
    public SearchHit(IndexerInformations ii, String id, long version, float score, Map<String, Object> data) 
    {
        super(ii, id, version);
        
        this.score = score;
        setData(data);
    }

    public float getScore() 
    {
        return score;
    }
}
