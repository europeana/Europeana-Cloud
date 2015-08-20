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
            "DC_EXTRACTOR",
            "dc_extractor",
            "Dc_Extractor",
            "DefaultExtractor",
            "gfdslfs",
            ""
        };
        
        OaiExtractionMethods method;
                
        for(String s: strings)
        {
            method = OaiExtractionMethods.DC_EXTRACTOR.getMethod(s);
            assertEquals(OaiExtractionMethods.DC_EXTRACTOR, method);
        }
        
        method = OaiExtractionMethods.DC_EXTRACTOR.getMethod(null);
        assertEquals(OaiExtractionMethods.DC_EXTRACTOR, method);
    }
}
