package eu.europeana.cloud.service.dps.index.structure;

import eu.europeana.cloud.service.dps.index.SupportedIndexers;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Pavel Kefurt <Pavel.Kefurt@gmail.com>
 */
public class IndexerInformations 
{
    private List<String> addresses;
    
    private final String index;
    private final String type;
    
    private final SupportedIndexers indexer;
    
    enum MapFields
    {
        INDEXER_NAME,
        INDEX,
        TYPE
    }
    
    private static final Logger LOGGER = LoggerFactory.getLogger(IndexerInformations.class);
    
    public IndexerInformations(String indexer, String index, String type) 
    {
        this.index = index;
        this.type = type;
        this.indexer = SupportedIndexers.fromString(indexer);
        this.addresses = new ArrayList<>();
    }
    
    public IndexerInformations(String indexer, String index, String type, String addresses) 
    {
        this.index = index;
        this.type = type;
        this.indexer = SupportedIndexers.fromString(indexer);
        
        setAddresses(addresses);
    }
    
    public IndexerInformations (Map<String, String> info)
    {
        String _indexer = null;
        String _index = null;
        String _type = null;
        
        for(Map.Entry<String, String> e: info.entrySet())
        {
            MapFields key;            
            try
            {
                key = MapFields.valueOf(e.getKey().toUpperCase());
            }
            catch(IllegalArgumentException ex)
            {
                continue;
            }
         
            switch(key)
            {
                case INDEXER_NAME:
                    _indexer = e.getValue();
                    break;
                case INDEX:
                    _index = e.getValue();
                    break;
                case TYPE:
                    _type = e.getValue();
                    break;
            }
        }
        
        this.index = _index;
        this.type = _type;
        this.indexer = SupportedIndexers.fromString(_indexer); 
        this.addresses = new ArrayList<>();
    }

    public final void setAddresses(String addresses)
    {
        this.addresses = new ArrayList<>();
        
        if(addresses != null)
        {
            String[] a = addresses.split(";");

            for(String address: a)
            {
                if(address != null && !address.isEmpty())
                {
                    this.addresses.add(address);
                }
            }
        }
    }
    
    public String toTaskString()
    {
        Map<String, String> tmp = new HashMap<>();
        tmp.put(MapFields.INDEXER_NAME.toString(), indexer.toString());
        tmp.put(MapFields.INDEX.toString(), index);
        tmp.put(MapFields.TYPE.toString(), type);
        
        try 
        {
            return new ObjectMapper().writeValueAsString(tmp);
        } 
        catch (IOException ex) 
        {
            LOGGER.warn("Cannot write indexer informations because: {}", ex.getMessage());
            return null;
        }
    }
    
    public static IndexerInformations fromTaskString(String data)
    {
        if(data == null || data.isEmpty())
        {
            return null;
        }
        
        Map<String, String> parameters;
        try 
        {
            parameters = new ObjectMapper().readValue(data, HashMap.class);
        } 
        catch (IOException ex) 
        {
            LOGGER.warn("Cannot read indexer informations because: {}", ex.getMessage());
            return null;
        }
        
        return new IndexerInformations(parameters);
    }
    
    public SupportedIndexers getIndexerName()
    {
        return indexer;
    }
    
    public List<String> getAddresses() 
    {
        return addresses;
    }

    public String getIndex() 
    {
        return index;
    }

    public String getType() 
    {
        return type;
    }
}
