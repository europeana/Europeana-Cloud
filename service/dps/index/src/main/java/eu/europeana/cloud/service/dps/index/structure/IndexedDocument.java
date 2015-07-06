package eu.europeana.cloud.service.dps.index.structure;

import java.util.Map;

/**
 *
 * @author Pavel Kefurt <Pavel.Kefurt@gmail.com>
 */
public class IndexedDocument 
{
    private final IndexerInformations indexerInformations;
    private final String id;
    private final long version;
    
    private Map<String, Object> data;

    public IndexedDocument(IndexerInformations ii, String id, long version) 
    {
        this.indexerInformations = ii;
        this.id = id;
        this.version = version;
    }

    public Map<String, Object> getData() 
    {
        return data;
    }

    public void setData(Map<String, Object> data) 
    {
        this.data = data;
    }

    public IndexerInformations getIndexerInformations() 
    {
        return indexerInformations;
    }

    public String getId() 
    {
        return id;
    }

    public long getVersion() 
    {
        return version;
    }
}
