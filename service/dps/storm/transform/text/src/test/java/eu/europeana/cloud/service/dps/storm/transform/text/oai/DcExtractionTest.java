package eu.europeana.cloud.service.dps.storm.transform.text.oai;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;
import static org.junit.Assert.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Pavel Kefurt <Pavel.Kefurt@gmail.com>
 */
public class DcExtractionTest 
{    
    private static final Logger LOGGER = LoggerFactory.getLogger(DcExtractionTest.class);
    
    private final String rightFileContent = "C:\\Users\\ceffa\\Desktop\\OU_tmp\\rightDcTestFile.xml";
    private final String wrongFileContent1 = "C:\\Users\\ceffa\\Desktop\\OU_tmp\\wrongDcTestFile.xml";
    private final String wrongFileContent2 = "C:\\Users\\ceffa\\Desktop\\OU_tmp\\wrongDcXmlDocument.xml";
    private final String wrongFileContent3 = "C:\\Users\\ceffa\\Desktop\\OU_tmp\\wrongXmlDocument.xml";
    private final String wrongFileContent4 = "C:\\Users\\ceffa\\Desktop\\OU_tmp\\Koala.jpg";
   
    @Test
    public void readRightFileWithDefaultFiealdsTest()
    {
        DcExtractor extractor = new DcExtractor();
        
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
        
        String exceptedJson = "{\"date\":\"2012-10-30\",\"creator\":\"Creator\",\"format\":\"application/pdf\",\"description\":\"Dextription\",\"publisher\":\"Publisher\",\"language\":\"eng\",\"title\":\"Title\",\"type\":[\"Type1\",\"Type2\"]}";
        
        assertEquals(exceptedJson, extracted);
    }
    
    @Test
    public void readRightFileWithCustomFieldsTest()
    {
        Map<String, String> fields = new HashMap<>();
        fields.put("myDate", "dc:date");
        fields.put("myCreator", "dc:creator");
        fields.put("format", "dc:format");
        
        DcExtractor extractor = new DcExtractor(fields);
        
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
        
        String exceptedJson = "{\"myDate\":\"2012-10-30\",\"myCreator\":\"Creator\",\"format\":\"application/pdf\"}";
        
        assertEquals(exceptedJson, extracted);
    }
    
    @Test
    public void readRightFileWithWrongCustomFieldsTest()
    {
        Map<String, String> fields = new HashMap<>();
        fields.put("myDate", "dc:date");
        fields.put("myCreator", "creator");
        fields.put("format", "dc:format");
        fields.put("types", "type");
        fields.put("notExist", "dc:somethingExtra__");
        
        DcExtractor extractor = new DcExtractor(fields);
        
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
        
        String exceptedJson = "{\"myDate\":\"2012-10-30\",\"format\":\"application/pdf\"}";
        
        assertEquals(exceptedJson, extracted);
    }
    
    @Test
    public void readWrongDcFileTest()
    {
        DcExtractor extractor = new DcExtractor();
        
        FileInputStream is;
        try
        {
            is = new FileInputStream(wrongFileContent1);
        } 
        catch (FileNotFoundException ex) 
        {
            LOGGER.error("File {} is not found!", wrongFileContent1);
            return;
        }
        String extracted = extractor.extractText(is);
        
        String exceptedJson = "{\"creator\":\"Creator\",\"format\":\"application/pdf\",\"description\":\"Dextription\",\"type\":[\"Type2\"]}";
        
        assertEquals(exceptedJson, extracted);
    }
    
    @Test
    public void readWrongDcDocumentTest()
    {
        DcExtractor extractor = new DcExtractor();
        
        FileInputStream is;
        try
        {
            is = new FileInputStream(wrongFileContent2);
        } 
        catch (FileNotFoundException ex) 
        {
            LOGGER.error("File {} is not found!", wrongFileContent2);
            return;
        }
        String extracted = extractor.extractText(is);
        
        assertNull(extracted);
    }
    
    @Test
    public void readWrongXmlDocumentTest()
    {
        DcExtractor extractor = new DcExtractor();
        
        FileInputStream is;
        try
        {
            is = new FileInputStream(wrongFileContent3);
        } 
        catch (FileNotFoundException ex) 
        {
            LOGGER.error("File {} is not found!", wrongFileContent3);
            return;
        }
        String extracted = extractor.extractText(is);
        
        assertNull(extracted);
    }
    
    @Test
    public void readNonxmlDocumentTest()
    {
        DcExtractor extractor = new DcExtractor();
        
        FileInputStream is;
        try
        {
            is = new FileInputStream(wrongFileContent4);
        } 
        catch (FileNotFoundException ex) 
        {
            LOGGER.error("File {} is not found!", wrongFileContent4);
            return;
        }
        String extracted = extractor.extractText(is);
        
        assertNull(extracted);
    }
    
    @Test
    public void readNullTest()
    {
        DcExtractor extractor = new DcExtractor();
        
        String extracted = extractor.extractText(null);
        
        assertNull(extracted);
    }
}
