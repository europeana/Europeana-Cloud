package eu.europeana.cloud.service.dps.storm.transform.text;

import java.io.InputStream;

/**
 * Interface for extracting methods
 * @author Pavel Kefurt <Pavel.Kefurt@gmail.com>
 */
public interface TextExtractor 
{
    public String extractText(InputStream is);
    
    public ExtractionMethods getExtractorMethod();
}
