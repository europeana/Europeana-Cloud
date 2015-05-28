package eu.europeana.cloud.service.dps.storm.transform.text.oai;

import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author Pavel Kefurt <Pavel.Kefurt@gmail.com>
 */
public class DcExtractionTest 
{    
    
    private final String rightFileContent = "/rightDcTestFile.xml";
    private final String wrongFileContent1 = "/wrongDcTestFile.xml";
    private final String wrongFileContent2 = "/wrongDcXmlDocument.xml";
    private final String wrongFileContent3 = "/wrongXmlDocument.xml";
    private final String wrongFileContent4 = "/Koala.jpg";
   
    @Test
    public void readRightFileWithDefaultFiealdsTest()
    {
        DcExtractor extractor = new DcExtractor();
        
        InputStream is = getClass().getResourceAsStream(rightFileContent);

        String extracted = extractor.extractText(is);
        
        String expectedJson = "{\"date\":\"2012-10-30\",\"creator\":\"Creator\",\"format\":\"application/pdf\",\"description\":\"Dextription\",\"publisher\":\"Publisher\",\"language\":\"eng\",\"title\":\"Title\",\"type\":[\"Type1\",\"Type2\"]}";
        JsonElement expectedObject = new JsonParser().parse(expectedJson);
        JsonElement extractedObject = new JsonParser().parse(extracted);
        
        assertEquals(expectedObject, extractedObject);
    }
    
    @Test
    public void readRightFileWithCustomFieldsTest()
    {
        Map<String, String> fields = new HashMap<>();
        fields.put("myDate", "dc:date");
        fields.put("myCreator", "dc:creator");
        fields.put("format", "dc:format");
        
        DcExtractor extractor = new DcExtractor(fields);
        
        InputStream is = getClass().getResourceAsStream(rightFileContent);

        String extracted = extractor.extractText(is);
        
        String expectedJson = "{\"myDate\":\"2012-10-30\",\"myCreator\":\"Creator\",\"format\":\"application/pdf\"}";
        JsonElement expectedObject = new JsonParser().parse(expectedJson);
        JsonElement extractedObject = new JsonParser().parse(extracted);
        
        assertEquals(expectedObject, extractedObject);
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
        
        InputStream is = getClass().getResourceAsStream(rightFileContent);

        String extracted = extractor.extractText(is);
        
        String expectedJson = "{\"myDate\":\"2012-10-30\",\"format\":\"application/pdf\"}";
        JsonElement expectedObject = new JsonParser().parse(expectedJson);
        JsonElement extractedObject = new JsonParser().parse(extracted);
        
        assertEquals(expectedObject, extractedObject);
    }
    
    @Test
    public void readWrongDcFileTest()
    {
        DcExtractor extractor = new DcExtractor();
        
        InputStream is = getClass().getResourceAsStream(wrongFileContent1);

        String extracted = extractor.extractText(is);
        
        String expectedJson = "{\"creator\":\"Creator\",\"format\":\"application/pdf\",\"description\":\"Dextription\",\"type\":[\"Type2\"]}";
        JsonElement expectedObject = new JsonParser().parse(expectedJson);
        JsonElement extractedObject = new JsonParser().parse(extracted);
        
        assertEquals(expectedObject, extractedObject);
    }
    
    @Test
    public void readWrongDcDocumentTest()
    {
        DcExtractor extractor = new DcExtractor();
        
        InputStream is = getClass().getResourceAsStream(wrongFileContent2);

        String extracted = extractor.extractText(is);
        
        assertNull(extracted);
    }
    
    @Test
    public void readWrongXmlDocumentTest()
    {
        DcExtractor extractor = new DcExtractor();
        
        InputStream is = getClass().getResourceAsStream(wrongFileContent3);

        String extracted = extractor.extractText(is);
        
        assertNull(extracted);
    }
    
    @Test
    public void readNonxmlDocumentTest()
    {
        DcExtractor extractor = new DcExtractor();
        
        InputStream is = getClass().getResourceAsStream(wrongFileContent4);

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
