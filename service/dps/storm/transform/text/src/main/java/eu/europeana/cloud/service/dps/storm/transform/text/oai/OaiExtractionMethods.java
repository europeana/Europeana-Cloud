package eu.europeana.cloud.service.dps.storm.transform.text.oai;

import eu.europeana.cloud.service.dps.storm.transform.text.MethodsEnumeration;

/**
 * Supported extraction methods for OAI.
 * @author Pavel Kefurt <Pavel.Kefurt@gmail.com>
 */
public enum OaiExtractionMethods implements MethodsEnumeration<OaiExtractionMethods>
{
    DC_EXTRACTOR; //default extractor
    
    @Override
    public OaiExtractionMethods getMethod(String value) 
    {
        OaiExtractionMethods ret;
        try
        {
            ret = valueOf(value.toUpperCase());
        }
        catch(IllegalArgumentException | NullPointerException ex)
        {
           return DC_EXTRACTOR; //defaul enum constant
        }
        
        return ret;
    }
}
