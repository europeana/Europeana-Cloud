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
 * Object with informations about indexer.
 * @author Pavel Kefurt <Pavel.Kefurt@gmail.com>
 */
public class IndexerInformations 
{
    private List<String> addresses;
    
    private final String index;
    private final String type;
    
    private final SupportedIndexers indexer;
    
    public enum MapFields
    {
        INDEXER_NAME,
        INDEX,
        TYPE
    }
    
    private static final Logger LOGGER = LoggerFactory.getLogger(IndexerInformations.class);
    
    /**
     * Construct object without servers addresses.
     * @param indexer name of indexer ({@link eu.europeana.cloud.service.dps.index.SupportedIndexers SupportedIndexers})
     * @param index index name
     * @param type type name ({@link eu.europeana.cloud.service.dps.index.Solr Solr} indexer ignore this parameter)
     */
    public IndexerInformations(String indexer, String index, String type) 
    {
        this.index = index;
        this.type = type;
        this.indexer = SupportedIndexers.fromString(indexer);
        this.addresses = new ArrayList<>();
    }
    
    /**
     * Construct object with servers addresses.
     * @param indexer name of indexer ({@link eu.europeana.cloud.service.dps.index.SupportedIndexers SupportedIndexers})
     * @param index index name
     * @param type type name ({@link eu.europeana.cloud.service.dps.index.Solr Solr} indexer ignore this parameter)
     * @param addresses indexer servers addresses (separated by semicolon)
     */
    public IndexerInformations(String indexer, String index, String type, String addresses) 
    {
        this.index = index;
        this.type = type;
        this.indexer = SupportedIndexers.fromString(indexer);
        
        setAddresses(addresses);
    }
    
    /**
     * Construct object from Map.
     * @param info informations about indexer
     *      Key is {@link eu.europeana.cloud.service.dps.index.structure.IndexerInformations.MapFields MapFields}
     */
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

    /**
     * Set indexer servers addresses.
     * @param addresses addresses separated by semicolon (;)
     */
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
    
    /**
     * Retrieve string representation of indexer+index+type for index identification
     * @return index identification
     */
    public String toKey()
    {
        switch(indexer)
        {
            case ELASTICSEARCH_INDEXER:
                return indexer.toString() + index.toLowerCase() + type.toLowerCase();
            case SOLR_INDEXER:
                return indexer.toString() + index.toLowerCase();
            case UNSUPPORTED:
            default:
                return indexer.toString();
        }
    }
    
    /**
     * Serialize this instance.
     * @return JSON representation of this instance
     */
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
    
    /**
     * Deserialize instance of this object
     * @param data JSON string
     * @return instance of IndexerInformations
     */
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
