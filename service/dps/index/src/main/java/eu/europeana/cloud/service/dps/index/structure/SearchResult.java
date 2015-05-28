package eu.europeana.cloud.service.dps.index.structure;

import java.util.Map;


/**
 *
 * @author Pavel Kefurt <Pavel.Kefurt@gmail.com>
 */
public class SearchResult extends IndexedDocument
{
    private final float score;

    public SearchResult(String index, String type, String id, long version, float score, Map<String, Object> data) 
    {
        super(index, type, id, version);
        
        this.score = score;
        setData(data);
    }

    public float getScore() 
    {
        return score;
    }
}
