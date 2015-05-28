package eu.europeana.cloud.service.dps.storm.transform.text.pdf;

import java.io.InputStream;
import java.util.Map;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author Pavel Kefurt <Pavel.Kefurt@gmail.com>
 */
public class TikaExtractionTest 
{   
    private final String exceptedTextInFile = "This is a text!";
    
    private final String rightFileContent = "/rightTestFile.pdf";
    private final String wrongFileContent = "/Koala.jpg";

    @Test
    public void readRightFileTest()
    {
        TikaExtractor extractor = new TikaExtractor();
        
        InputStream is = getClass().getResourceAsStream(rightFileContent);

        String extracted = extractor.extractText(is);

        assertEquals(exceptedTextInFile, extracted.trim());
    }
    
    @Test
    public void readWrongFileTest()
    {
        TikaExtractor extractor = new TikaExtractor();
        
        InputStream is = getClass().getResourceAsStream(wrongFileContent);

        String extracted = extractor.extractText(is);
        
        assertNull(extracted);
    }
    
    @Test
    public void readMetadataFromFileAfterExtractionTest()
    {
        TikaExtractor extractor = new TikaExtractor();
        
        InputStream is = getClass().getResourceAsStream(rightFileContent);

        extractor.extractText(is);
        Map<String, String> extractedMetadata = extractor.getExtractedMetadata();
        
        assertNotNull(extractedMetadata);
    }
    
    @Test
    public void readMetadataFromFileBeforeExtractionTest()
    {
        TikaExtractor extractor = new TikaExtractor();
        
        InputStream is = getClass().getResourceAsStream(rightFileContent);
  
        Map<String, String> extractedMetadata = extractor.getExtractedMetadata();
        
        assertNull(extractedMetadata);
    }
    
    @Test
    public void readNullTest()
    {
        TikaExtractor extractor = new TikaExtractor();
        
        String extracted = extractor.extractText(null);
        
        assertNull(extracted);
    }
    
    @Test
    public void readProperitiesFromExtractor()
    {
        TikaExtractor extractor = new TikaExtractor();
        
        assertEquals(PdfExtractionMethods.TIKA_EXTRACTOR, extractor.getExtractorMethod());
        
        String name = extractor.getRepresentationName();
        
        assertNotNull(name);
        assertFalse(name.isEmpty());
    }
}
