package eu.europeana.cloud.service.dps.storm.transform.text.txt;

import java.io.InputStream;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertFalse;
import org.junit.Test;

/**
 *
 * @author Pavel Kefurt <Pavel.Kefurt@gmail.com>
 */
public class ReadFileExtractionTest 
{
    private final String textInFiles = "File text content with encoding.";
    
    private final String utf8Text = "/utf8-file.txt";
    private final String asciiText = "/ascii-file.txt";
    
    @Test
    public void readUtf8Test()
    {
        ReadFileExtractor extractor = new ReadFileExtractor();
        
        InputStream is = getClass().getResourceAsStream(utf8Text);

        String extracted = extractor.extractText(is);
        
        assertEquals(textInFiles, extracted.substring(1).trim());   //first char is BOM
    }
    
    @Test
    public void readAsciiTest()
    {
        ReadFileExtractor extractor = new ReadFileExtractor();
        
        InputStream is = getClass().getResourceAsStream(asciiText);

        String extracted = extractor.extractText(is);

        assertEquals(textInFiles, extracted.trim());
    }
    
    @Test
    public void readNullTest()
    {
        ReadFileExtractor extractor = new ReadFileExtractor();

        String extracted = extractor.extractText(null);

        assertNull(extracted);
    }
    
    @Test
    public void readProperitiesFromExtractor()
    {
        ReadFileExtractor extractor = new ReadFileExtractor();
        
        assertEquals(TxtExtractionMethods.READ_FILE_EXTRACTOR, extractor.getExtractorMethod());
        
        String name = extractor.getRepresentationName();
        
        assertNotNull(name);
        assertFalse(name.isEmpty());
    }
}
