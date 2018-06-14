package eu.europeana.cloud.service.dps.storm.transform.text.edm;

import org.junit.Assert;
import org.junit.Test;

import java.io.InputStream;

import static org.junit.Assert.assertNull;

/**
 *
 * @author Pavel Kefurt <Pavel.Kefurt@gmail.com>
 */
public class JibxExtractionTest 
{
    private final String rightFileContent = "/rightEdmTestFile.xml";
    private final String wrongFileContent1 = "/wrongEdmTestFile.xml";
    private final String wrongFileContent2 = "/wrongXmlDocument.xml";
    private final String wrongFileContent3 = "/Koala.jpg";
    
    @Test
    public void readRightFileTest()
    {
        JibxExtractor extractor = new JibxExtractor();
        
        InputStream is = getClass().getResourceAsStream(rightFileContent);

        String extracted = extractor.extractText(is);
        
        Assert.assertNotNull(extracted);
        Assert.assertTrue(extracted.length() > 0);
    }
    
    @Test
    public void readWrongEdmDocumentTest()
    {
        JibxExtractor extractor = new JibxExtractor();
        
        InputStream is = getClass().getResourceAsStream(wrongFileContent1);

        String extracted = extractor.extractText(is);
        
        assertNull(extracted);
    }
    
    @Test
    public void readWrongXmlDocumentTest()
    {
        JibxExtractor extractor = new JibxExtractor();
        
        InputStream is = getClass().getResourceAsStream(wrongFileContent2);

        String extracted = extractor.extractText(is);
        
        assertNull(extracted);
    }
    
    @Test
    public void readNonxmlDocumentTest()
    {
        JibxExtractor extractor = new JibxExtractor();
        
        InputStream is = getClass().getResourceAsStream(wrongFileContent3);

        String extracted = extractor.extractText(is);
        
        assertNull(extracted);
    }
    
    @Test
    public void readNullTest()
    {
        JibxExtractor extractor = new JibxExtractor();
        
        String extracted = extractor.extractText(null);
        
        assertNull(extracted);
    }
}
