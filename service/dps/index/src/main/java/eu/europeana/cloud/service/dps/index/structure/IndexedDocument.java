package eu.europeana.cloud.service.dps.index.structure;

import java.util.Map;

/**
 *
 * @author Pavel Kefurt <Pavel.Kefurt@gmail.com>
 */
public class IndexedDocument 
{
    private final String index;
    private final String type;
    private final String id;
    private final long version;
    
    private Map<String, Object> data;

    public IndexedDocument(String index, String type, String id, long version) 
    {
        this.index = index;
        this.type = type;
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

    public String getIndex() 
    {
        return index;
    }

    public String getType() 
    {
        return type;
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
