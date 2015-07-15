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
    DESCRIPTION,
    _ALL;
    
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
    
    public static String[] toStringArray(IndexFields[] fields)
    {
        String[] newFields = new String[fields.length];
        int i = 0;
        for(IndexFields f: fields)
        {
            newFields[i] = fields[i].toString();
            i++;
        }
        
        return newFields;
    }
}
