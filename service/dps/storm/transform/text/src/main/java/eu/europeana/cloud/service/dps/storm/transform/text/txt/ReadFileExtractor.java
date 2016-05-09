package eu.europeana.cloud.service.dps.storm.transform.text.txt;

import eu.europeana.cloud.service.dps.storm.transform.text.MethodsEnumeration;
import eu.europeana.cloud.service.dps.storm.transform.text.TextExtractor;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Text extractor for TXT files that only read the file.
 * @author Pavel Kefurt <Pavel.Kefurt@gmail.com>
 */
public class ReadFileExtractor implements TextExtractor
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ReadFileExtractor.class);
    
    @Override
    public String extractText(InputStream is) 
    {
        if(is == null)
        {
            return null;
        }
        
        try 
        {
            return IOUtils.toString(is);
        } 
        catch (IOException ex) 
        {
            LOGGER.warn("Can not read text from txt file because: " + ex.getMessage()); 
            return null;
        }
    }

    @Override
    public MethodsEnumeration getExtractionMethod() 
    {
        return TxtExtractionMethods.READ_FILE_EXTRACTOR;
    }

    @Override
    public Map<String, String> getExtractedMetadata() 
    {
        return null;   
    }

    @Override
    public String getRepresentationName() 
    {
        return "txt";
    } 
}
