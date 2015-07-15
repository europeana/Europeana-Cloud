package eu.europeana.cloud.service.dps.index.structure;

import java.util.ArrayList;
import java.util.List;


/**
 *
 * @author Pavel Kefurt <Pavel.Kefurt@gmail.com>
 */
public class SearchResult
{
    public enum QueryTypes
    {
        MORE_LIKE_THIS,
        SEARCH,
        UNKNOWN;
    }
    
    private final long totalHits;
    private final float maxScore;
    private final long tookTime; //ms
     
    private Object query;
    private QueryTypes queryType;
    private String scrollId;
    
    private final List<SearchHit> hits;

    public SearchResult(List<SearchHit> hits, long totalHits, float maxScore, long tookTime)
    {
        this(hits, totalHits, maxScore, tookTime, null);
    }
    
    public SearchResult(List<SearchHit> hits, long totalHits, float maxScore, long tookTime, String scrollId) 
    {
        this.totalHits = totalHits;
        this.maxScore = maxScore;
        this.tookTime = tookTime;
        
        if(hits != null)
        {
            this.hits = hits;
        }
        else
        {
            this.hits = new ArrayList<>();
        }
        
        this.query = null;
        this.queryType = QueryTypes.UNKNOWN;
        this.scrollId = scrollId;
    }

    public long getTotalHits() 
    {
        return totalHits;
    }

    public long getTookTime() 
    {
        return tookTime;
    }

    public List<SearchHit> getHits() 
    {
        return hits;
    }
    
    public float getMaxScore() 
    {
        return maxScore;
    }

    public String getScrollId() 
    {
        return scrollId;
    } 
    
    public void setScrollId(String id)
    {
        this.scrollId = id;
    }

    public Object getQuery() {
        return query;
    }

    public void setQuery(Object query) 
    {
        this.query = query;
    }

    public QueryTypes getQueryType() 
    {
        return queryType;
    }

    public void setQueryType(QueryTypes queryType) 
    {
        this.queryType = queryType;
    }   
}
