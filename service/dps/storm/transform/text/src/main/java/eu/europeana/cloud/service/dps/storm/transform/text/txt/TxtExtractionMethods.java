package eu.europeana.cloud.service.dps.storm.transform.text.txt;

import eu.europeana.cloud.service.dps.storm.transform.text.MethodsEnumeration;

/**
 * Supported extraction methods for TXT.
 * @author Pavel Kefurt <Pavel.Kefurt@gmail.com>
 */
public enum TxtExtractionMethods implements MethodsEnumeration<TxtExtractionMethods>
{
    READ_FILE_EXTRACTOR;

    @Override
    public TxtExtractionMethods getMethod(String value) 
    {
        TxtExtractionMethods ret;
        try
        {
            ret = valueOf(value.toUpperCase());
        }
        catch(IllegalArgumentException | NullPointerException ex)
        {
           return READ_FILE_EXTRACTOR; //defaul enum constant
        }
        
        return ret;
    } 
}
