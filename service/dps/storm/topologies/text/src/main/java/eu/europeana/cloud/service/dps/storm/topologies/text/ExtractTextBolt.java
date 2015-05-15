package eu.europeana.cloud.service.dps.storm.topologies.text;

import backtype.storm.topology.OutputFieldsDeclarer;
import com.google.gson.Gson;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
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
    private final String storeStremName;
    private final String informStreamName;
    private static final Logger LOGGER = LoggerFactory.getLogger(ExtractTextBolt.class);
    
    public ExtractTextBolt(String storeStremName, String informStreamName) 
    {
       this.storeStremName = storeStremName;
       this.informStreamName = informStreamName;
    }
    
    @Override
    public void execute(StormTaskTuple t) 
    {
        String extractorName = t.getParameter(PluginParameterKeys.EXTRACTOR);
        String representationName = t.getParameter(PluginParameterKeys.REPRESENTATION_NAME);
        TextExtractor extractor = TextExtractorFactory.getExtractor(representationName, extractorName);
        
        if(extractor == null)
        {
            LOGGER.warn("Extractor not exists for representation name {}.", representationName);
            outputCollector.ack(inputTuple);
            return;
        }
        else
        {
            LOGGER.info("Renpresentation name: {}, Required extractor: {}, Selected extractor: {}", 
                    representationName, extractorName, extractor.getExtractorMethod().name());
        }
        
        ByteArrayInputStream data = t.getFileByteDataAsStream();
        String extractedText = extractor.extractText(data);
        Map<String, String> metadata = extractor.getExtractedMetadata(); 
        
        if(extractedText != null && !extractedText.isEmpty())
        {
            t.setFileData(extractedText);
            t.addParameter(PluginParameterKeys.MIME_TYPE, "text/plain");
            t.addParameter(PluginParameterKeys.REPRESENTATION_NAME, extractor.getRepresentationName());
            t.addParameter(PluginParameterKeys.ORIGINAL_FILE_URL, t.getFileUrl());
            
            if(metadata != null && !metadata.isEmpty())
            {
                t.addParameter(PluginParameterKeys.FILE_METADATA, new Gson().toJson(metadata));
            }
            
            //store extracted text?
            if (Boolean.parseBoolean(t.getParameter(PluginParameterKeys.STORE_EXTRACTED_TEXT)))
            {
                outputCollector.emit(storeStremName, inputTuple, t.toStormTuple());   
            }
            else
            {
                outputCollector.emit(informStreamName, inputTuple, t.toStormTuple()); 
            }
        }
        outputCollector.ack(inputTuple);
    }

    @Override
    public void prepare() 
    {}  

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) 
    {
        //store branch
        declarer.declareStream(storeStremName, StormTaskTuple.getFields());
        
        //inform branch
        declarer.declareStream(informStreamName, StormTaskTuple.getFields());
    }
}
