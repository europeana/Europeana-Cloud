package eu.europeana.cloud.service.dps.storm.transform.text.pdf;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.Map;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Pavel Kefurt <Pavel.Kefurt@gmail.com>
 */
public class PdfBoxExtractionTest 
{
    private static final Logger LOGGER = LoggerFactory.getLogger(PdfBoxExtractionTest.class);
    
    private final String exceptedTextInFile = "This is a text! \r\n";
    
    private final String rightFileContent = "C:\\Users\\ceffa\\Desktop\\OU_tmp\\rightTestFile.pdf";
    private final String wrongFileContent = "C:\\Users\\ceffa\\Desktop\\OU_tmp\\Koala.jpg";

    @Test
    public void readRightFileTest()
    {
        PdfBoxExtractor extractor = new PdfBoxExtractor();
        
        FileInputStream is;
        try
        {
            is = new FileInputStream(rightFileContent);
        } 
        catch (FileNotFoundException ex) 
        {
            LOGGER.error("File {} is not found!", rightFileContent);
            return;
        }
        String extracted = extractor.extractText(is);
        
        assertEquals(exceptedTextInFile, extracted);
    }
    
    @Test
    public void readWrongFileTest()
    {
        PdfBoxExtractor extractor = new PdfBoxExtractor();
        
        FileInputStream is;
        try
        {
            is = new FileInputStream(wrongFileContent);
        } 
        catch (FileNotFoundException ex) 
        {
            LOGGER.error("File {} is not found!", wrongFileContent);
            return;
        }
        String extracted = extractor.extractText(is);
        
        assertNull(extracted);
    }
    
    @Ignore
    @Test
    public void readMetadataFromFileAfterExtractionTest()
    {
        PdfBoxExtractor extractor = new PdfBoxExtractor();
        
        FileInputStream is;
        try
        {
            is = new FileInputStream(rightFileContent);
        } 
        catch (FileNotFoundException ex) 
        {
            LOGGER.error("File {} is not found!", rightFileContent);
            return;
        }
        extractor.extractText(is);
        Map<String, String> extractedMetadata = extractor.getExtractedMetadata();
        
        assertNotNull(extractedMetadata);
    }
    
    @Test
    public void readMetadataFromFileBeforeExtractionTest()
    {
        PdfBoxExtractor extractor = new PdfBoxExtractor();
        
        FileInputStream is;
        try
        {
            is = new FileInputStream(rightFileContent);
        } 
        catch (FileNotFoundException ex) 
        {
            LOGGER.error("File {} is not found!", rightFileContent);
            return;
        }
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
        
        assertEquals(PdfExtractionMethods.PDFBOX_EXTRACTOR, extractor.getExtractorMethod());
        
        String name = extractor.getRepresentationName();
        
        assertNotNull(name);
        assertFalse(name.isEmpty());
    }   
}
