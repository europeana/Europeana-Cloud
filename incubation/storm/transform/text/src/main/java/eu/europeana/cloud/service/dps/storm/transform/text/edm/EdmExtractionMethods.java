package eu.europeana.cloud.service.dps.storm.transform.text.edm;

import eu.europeana.cloud.service.dps.storm.transform.text.MethodsEnumeration;

/**
 * Supported extraction methods for EDM.
 * @author Pavel Kefurt <Pavel.Kefurt@gmail.com>
 */
public enum EdmExtractionMethods implements MethodsEnumeration<EdmExtractionMethods>
{
    JIBX_EXTRACTOR;

    @Override
    public EdmExtractionMethods getMethod(String value) 
    {
        EdmExtractionMethods ret;
        try
        {
            ret = valueOf(value.toUpperCase());
        }
        catch(IllegalArgumentException | NullPointerException ex)
        {
           return JIBX_EXTRACTOR; //defaul enum constant
        }
        
        return ret;
    } 
}
