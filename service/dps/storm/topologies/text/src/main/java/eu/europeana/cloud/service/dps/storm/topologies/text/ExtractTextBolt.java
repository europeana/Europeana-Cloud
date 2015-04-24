package eu.europeana.cloud.service.dps.storm.topologies.text;

import com.google.gson.Gson;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.dps.storm.transform.text.PdfBoxExtractor;
import eu.europeana.cloud.service.dps.storm.transform.text.TextExtractor;
import eu.europeana.cloud.service.dps.storm.transform.text.TextExtractorFactory;
import java.io.ByteArrayInputStream;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Bolt for text extracting.
 * It uses {@link DpsTask} parameter with key {@link PluginParameterKeys.EXTRACTOR} for determine which method should be used.
 * @author Pavel Kefurt <Pavel.Kefurt@gmail.com>
 */
public class ExtractTextBolt extends AbstractDpsBolt
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ExtractTextBolt.class);
    
    @Override
    public void execute(StormTaskTuple t) 
    {
        String extractorName = t.getParameter(PluginParameterKeys.EXTRACTOR);
        TextExtractor extractor = TextExtractorFactory.getExtractor(extractorName);
        
        LOGGER.info("Required extractor: {}, Selected extractor: {}", extractorName, extractor.getExtractorMethod().name());

        ByteArrayInputStream data = t.getFileByteDataAsStream();
        String extractedText = extractor.extractText(data);
        Map<String, String> metadata = extractor.getExtractedMetadata(); 
        
        if(extractedText != null && !extractedText.isEmpty())
        {
            t.setFileData(extractedText);
            t.addParameter(PluginParameterKeys.MIME_TYPE, "text/plain");
            t.addParameter(PluginParameterKeys.REPRESENTATION_NAME, TextStrippingConstants.EXTRACTED_TEXT_REPRESENTATION_NAME);
            t.addParameter(PluginParameterKeys.ORIGINAL_FILE_URL, t.getFileUrl());
            
            if(metadata != null && !metadata.isEmpty())
            {
                t.addParameter(PluginParameterKeys.FILE_METADATA, new Gson().toJson(metadata));
            }
            
            outputCollector.emit(inputTuple, t.toStormTuple());          
        }
        outputCollector.ack(inputTuple);
    }

    @Override
    public void prepare() 
    {}   
}
