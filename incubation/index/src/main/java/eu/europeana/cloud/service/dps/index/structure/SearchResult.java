package eu.europeana.cloud.service.dps.index.structure;

import java.util.ArrayList;
import java.util.List;


/**
 * Object representation of search result.
 * @author Pavel Kefurt <Pavel.Kefurt@gmail.com>
 */
public class SearchResult
{   
    private final long totalHits;
    private final float maxScore;
    private final long tookTime; //ms
     
    private Object query;
    private String scrollId;
    
    private final List<SearchHit> hits;

    /**
     * Construct object without scroll.
     * @param hits found documents
     * @param totalHits total number of results
     * @param maxScore best score
     * @param tookTime execute time in milliseconds 
     */
    public SearchResult(List<SearchHit> hits, long totalHits, float maxScore, long tookTime)
    {
        this(hits, totalHits, maxScore, tookTime, null);
    }
    
    /**
     * Construct object with scroll.
     * @param hits found documents
     * @param totalHits total number of results
     * @param maxScore best score
     * @param tookTime execute time in milliseconds 
     * @param scrollId scroll id for next page
     */
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

    public Object getQuery() 
    {
        return query;
    }

    public void setQuery(Object query) 
    {
        this.query = query;
    }  
}
