package eu.europeana.cloud.service.dps.storm.transform.text;

/**
 * Supported representations for extraction
 * @author Pavel Kefurt <Pavel.Kefurt@gmail.com>
 */
public enum SupportedRepresentations 
{
    PDF,
    OAI,
    TXT,
    EDM,
    UNSUPPORTED;    //default value
    
    /**
     * Retrieve the enum constant of representation. 
     * @param value String with potential enum constant (accept also null and unknown string)
     * @return enum constant
     */
    public static SupportedRepresentations getMethod(String value)
    {
        SupportedRepresentations ret;
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
