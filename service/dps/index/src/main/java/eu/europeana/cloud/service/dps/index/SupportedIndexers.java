package eu.europeana.cloud.service.dps.index;

/**
 *
 * @author Pavel Kefurt <Pavel.Kefurt@gmail.com>
 */
public enum SupportedIndexers 
{
    ELASTICSEARCH_INDEXER,
    SOLR_INDEXER,
    UNSUPPORTED;
    
    /**
     * Retrieve the enum constant of indexer. 
     * @param value String with potential enum constant (accepts also null and unknown string)
     * @return enum constant
     */
    public static SupportedIndexers fromString(String value)
    {
        SupportedIndexers ret;
        try
        {
            ret = valueOf(value.toUpperCase());
        }
        catch(IllegalArgumentException | NullPointerException ex)
        {
           return UNSUPPORTED;  //use default value
        }
        
        return ret;
    }
}
