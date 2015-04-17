package eu.europeana.cloud.service.dps.storm.transform.text;

/**
 * Supported extraction methods
 * @author Pavel Kefurt <Pavel.Kefurt@gmail.com>
 */
public enum ExtractionMethods 
{
    TIKA_EXTRACTOR,     //default extractor
    JPOD_EXTRACTOR,
    PDFBOX_EXTRACTOR;
    
    /**
     * Retrieve the enum constant of extraction metod. 
     * If not possible to recognize the enum name, than it will be used default enum.
     * @param value String with potential enum constant (accept also null and unknown string)
     * @return enum constant
     */
    public static ExtractionMethods getMethod(String value)
    {
        ExtractionMethods ret;
        try
        {
            ret = valueOf(value);
        }
        catch(IllegalArgumentException | NullPointerException ex)
        {
           return TIKA_EXTRACTOR; //defaul enum constant
        }
        
        return ret;
    }
}
