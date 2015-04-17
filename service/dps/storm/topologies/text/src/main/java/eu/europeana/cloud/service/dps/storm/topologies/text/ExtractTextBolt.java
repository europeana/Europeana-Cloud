package eu.europeana.cloud.service.dps.storm.topologies.text;

import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.dps.storm.transform.text.PdfBoxExtractor;
import eu.europeana.cloud.service.dps.storm.transform.text.TextExtractor;
import eu.europeana.cloud.service.dps.storm.transform.text.TextExtractorFactory;
import java.io.ByteArrayInputStream;
import org.apache.commons.codec.binary.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Bolt for text extracting.
 * It uses {@link DpsTask} parameter with key {@link PluginParameterKeys.EXTRACTOR} for determine which method should be used.
 * @author Pavel Kefurt <Pavel.Kefurt@gmail.com>
 */
public class ExtractTextBolt extends AbstractDpsBolt
{
    public static final Logger LOGGER = LoggerFactory.getLogger(ExtractTextBolt.class);
    
    @Override
    public void execute(StormTaskTuple t) 
    {
        String extractorName = t.getParameter(PluginParameterKeys.EXTRACTOR);
        TextExtractor extractor = TextExtractorFactory.getExtractor(extractorName);
        
        LOGGER.info("Required extractor: {}, Selected extractor: {}", extractorName, extractor.getExtractorMethod().name());

        ByteArrayInputStream data = new ByteArrayInputStream(Base64.decodeBase64(t.getFileByteData()));
        String extractedText = new PdfBoxExtractor().extractText(data);
        
        if(extractedText != null && !extractedText.isEmpty())
        {
            t.setFileData(extractedText);

            outputCollector.emit(inputTuple, t.toStormTuple());          
        }
        outputCollector.ack(inputTuple);
    }

    @Override
    public void prepare() 
    {}   
}
