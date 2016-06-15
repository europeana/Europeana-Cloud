package eu.europeana.cloud.service.dps.storm.transform.text;

import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author Pavel Kefurt <Pavel.Kefurt@gmail.com>
 */
public class SupportedRepresentationsTest 
{   
    @Test
    public void getUnsupportedRepresentationTest()
    {
        String[] strings = 
        {
            "UNSUPPORTED",
            "unsupported",
            "unknownRepresentation",
            "kfasdfd",
            ""
        };
        
        SupportedRepresentations method;
        
        for(String s: strings)
        {
            method = SupportedRepresentations.getMethod(s);
            assertEquals(SupportedRepresentations.UNSUPPORTED, method);
        }
        
        method = SupportedRepresentations.getMethod(null);
        assertEquals(SupportedRepresentations.UNSUPPORTED, method);
    }
    
    @Test
    public void getPdfRepresentationTest()
    {
        String[] strings = 
        {
            "PDF",
            "pdf",
            "Pdf",
            "pDf"
        };
        
        SupportedRepresentations method;
        
        for(String s: strings)
        {
            method = SupportedRepresentations.getMethod(s);
            assertEquals(SupportedRepresentations.PDF, method);
        }
    }
    
    @Test
    public void getOaiRepresentationTest()
    {
        String[] strings = 
        {
            "OAI",
            "oai",
            "Oai",
            "oAi"
        };
        
        SupportedRepresentations method;
        
        for(String s: strings)
        {
            method = SupportedRepresentations.getMethod(s);
            assertEquals(SupportedRepresentations.OAI, method);
        }
    }
    
    @Test
    public void getTxtRepresentationTest()
    {
        String[] strings = 
        {
            "TXT",
            "txt",
            "TxT",
            "TXt"
        };
        
        SupportedRepresentations method;
        
        for(String s: strings)
        {
            method = SupportedRepresentations.getMethod(s);
            assertEquals(SupportedRepresentations.TXT, method);
        }
    }
    
    @Test
    public void getEdmRepresentationTest()
    {
        String[] strings = 
        {
            "EDM",
            "edm",
            "EdM",
            "EDm"
        };
        
        SupportedRepresentations method;
        
        for(String s: strings)
        {
            method = SupportedRepresentations.getMethod(s);
            assertEquals(SupportedRepresentations.EDM, method);
        }
    }
}
