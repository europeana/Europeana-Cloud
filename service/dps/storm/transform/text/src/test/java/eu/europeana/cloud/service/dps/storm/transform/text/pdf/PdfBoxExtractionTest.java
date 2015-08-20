package eu.europeana.cloud.service.dps.storm.transform.text.pdf;

import java.io.InputStream;
import java.util.Map;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import org.junit.Ignore;
import org.junit.Test;

/**
 *
 * @author Pavel Kefurt <Pavel.Kefurt@gmail.com>
 */
public class PdfBoxExtractionTest 
{   
    private final String exceptedTextInFile = "This is a text!";
    
    private final String rightFileContent = "/rightTestFile.pdf";
    private final String wrongFileContent = "/Koala.jpg";

    @Test
    public void readRightFileTest()
    {
        PdfBoxExtractor extractor = new PdfBoxExtractor();
        
        InputStream is = getClass().getResourceAsStream(rightFileContent);

        String extracted = extractor.extractText(is);
        
        assertEquals(exceptedTextInFile, extracted.trim());
    }
    
    @Test
    public void readWrongFileTest()
    {
        PdfBoxExtractor extractor = new PdfBoxExtractor();
        
        InputStream is = getClass().getResourceAsStream(wrongFileContent);

        String extracted = extractor.extractText(is);
        
        assertNull(extracted);
    }
    
    @Ignore
    @Test
    public void readMetadataFromFileAfterExtractionTest()
    {
        PdfBoxExtractor extractor = new PdfBoxExtractor();
        
        InputStream is = getClass().getResourceAsStream(rightFileContent);

        extractor.extractText(is);
        Map<String, String> extractedMetadata = extractor.getExtractedMetadata();
        
        assertNotNull(extractedMetadata);
    }
    
    @Test
    public void readMetadataFromFileBeforeExtractionTest()
    {
        PdfBoxExtractor extractor = new PdfBoxExtractor();
        
        InputStream is = getClass().getResourceAsStream(rightFileContent);

        Map<String, String> extractedMetadata = extractor.getExtractedMetadata();
        
        assertNull(extractedMetadata);
    }
    
    @Test
    public void readNullTest()
    {
        PdfBoxExtractor extractor = new PdfBoxExtractor();
        
        String extracted = extractor.extractText(null);
        
        assertNull(extracted);
    }
    
    @Test
    public void readProperitiesFromExtractor()
    {
        PdfBoxExtractor extractor = new PdfBoxExtractor();
        
        assertEquals(PdfExtractionMethods.PDFBOX_EXTRACTOR, extractor.getExtractionMethod());
        
        String name = extractor.getRepresentationName();
        
        assertNotNull(name);
        assertFalse(name.isEmpty());
    }   
}
