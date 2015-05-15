package eu.europeana.cloud.service.dps.storm.transform.text.oai;

import static org.junit.Assert.assertEquals;
import org.junit.Test;

/**
 *
 * @author Pavel Kefurt <Pavel.Kefurt@gmail.com>
 */
public class OaiExtractorsTest 
{
    @Test
    public void getDcExtractorTest()
    {
        String[] strings = 
        {
            "DC",
            "dc",
            "Dc",
            "DefaultExtractor",
            "gfdslfs",
            ""
        };
        
        OaiExtractionMethods method;
                
        for(String s: strings)
        {
            method = OaiExtractionMethods.DC.getMethod(s);
            assertEquals(OaiExtractionMethods.DC, method);
        }
        
        method = OaiExtractionMethods.DC.getMethod(null);
        assertEquals(OaiExtractionMethods.DC, method);
    }
}
