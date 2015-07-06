package eu.europeana.cloud.service.dps.index;

/**
 *
 * @author Pavel Kefurt <Pavel.Kefurt@gmail.com>
 */
public enum IndexFields 
{
    RAW_TEXT,
    FILE_METADATA,
    TITLE,
    DESCRIPTION;
    
    /**
     * Retrieve the enum constant of index field. 
     * @param value String with potential enum constant (accepts also null and unknown string)
     * @return enum constant or null
     */
    public static IndexFields fromString(String value)
    {
        IndexFields ret;
        try
        {
            ret = valueOf(value.toUpperCase());
        }
        catch(IllegalArgumentException | NullPointerException ex)
        {
           return null;  //use default value
        }
        
        return ret;
    }

    @Override
    public String toString() 
    {
        return super.toString().toLowerCase();
    }
    
    
}
