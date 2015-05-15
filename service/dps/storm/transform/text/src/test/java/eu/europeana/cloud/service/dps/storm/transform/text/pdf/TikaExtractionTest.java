package eu.europeana.cloud.service.dps.storm.transform.text.pdf;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.Map;
import org.junit.Test;
import static org.junit.Assert.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Pavel Kefurt <Pavel.Kefurt@gmail.com>
 */
public class TikaExtractionTest 
{
    private static final Logger LOGGER = LoggerFactory.getLogger(TikaExtractionTest.class);
    
    private final String exceptedTextInFile = "\nThis is a text! \n\n\n";
    
    private final String rightFileContent = "C:\\Users\\ceffa\\Desktop\\OU_tmp\\rightTestFile.pdf";
    private final String wrongFileContent = "C:\\Users\\ceffa\\Desktop\\OU_tmp\\Koala.jpg";

    @Test
    public void readRightFileTest()
    {
        TikaExtractor extractor = new TikaExtractor();
        
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
        TikaExtractor extractor = new TikaExtractor();
        
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
    
    @Test
    public void readMetadataFromFileAfterExtractionTest()
    {
        TikaExtractor extractor = new TikaExtractor();
        
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
        TikaExtractor extractor = new TikaExtractor();
        
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
