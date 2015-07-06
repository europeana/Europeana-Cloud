package eu.europeana.cloud.service.dps.index.structure;

import java.util.Map;

/**
 *
 * @author Pavel Kefurt <Pavel.Kefurt@gmail.com>
 */
public class SearchHit extends IndexedDocument
{
    private final float score;

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
