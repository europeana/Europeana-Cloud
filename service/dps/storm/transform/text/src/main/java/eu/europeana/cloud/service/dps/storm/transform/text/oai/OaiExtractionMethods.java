package eu.europeana.cloud.service.dps.storm.transform.text.oai;

import eu.europeana.cloud.service.dps.storm.transform.text.MethodsEnumeration;

/**
 *
 * @author Pavel Kefurt <Pavel.Kefurt@gmail.com>
 */
public enum OaiExtractionMethods implements MethodsEnumeration<OaiExtractionMethods>
{
    DC; //default extractor
    
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
           return DC; //defaul enum constant
        }
        
        return ret;
    }
}
