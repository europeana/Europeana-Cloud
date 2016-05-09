package eu.europeana.cloud.service.dps.storm.transform.text.edm;

import static org.junit.Assert.assertEquals;
import org.junit.Test;

/**
 *
 * @author Pavel Kefurt <Pavel.Kefurt@gmail.com>
 */
public class EdmExtractorsTest 
{
    @Test
    public void getJibxExtractorTest()
    {
        String[] strings = 
        {
            "jibx_extractor",
            "JiBX_Extractor",
            "JIBX_EXTRACTOR",
            "DefaultExtractor",
            "gfdslfs",
            ""
        };
        
        EdmExtractionMethods method;
        
        for(String s: strings)
        {
            method = EdmExtractionMethods.JIBX_EXTRACTOR.getMethod(s);
            assertEquals(EdmExtractionMethods.JIBX_EXTRACTOR, method);
        }
        
        method = EdmExtractionMethods.JIBX_EXTRACTOR.getMethod(null);
        assertEquals(EdmExtractionMethods.JIBX_EXTRACTOR, method);
    }
}
