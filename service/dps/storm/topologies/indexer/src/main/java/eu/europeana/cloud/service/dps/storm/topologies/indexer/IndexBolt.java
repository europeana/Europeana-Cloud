package eu.europeana.cloud.service.dps.storm.topologies.indexer;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.index.IndexFields;
import eu.europeana.cloud.service.dps.index.Indexer;
import eu.europeana.cloud.service.dps.index.IndexerFactory;
import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.dps.util.LRUCache;
import eu.europeana.cloud.service.dps.index.SupportedIndexers;
import eu.europeana.cloud.service.dps.index.exception.IndexerException;
import eu.europeana.cloud.service.dps.index.structure.IndexerInformations;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Pavel Kefurt <Pavel.Kefurt@gmail.com>
 */
public class IndexBolt extends AbstractDpsBolt
{
    private static final Logger LOGGER = LoggerFactory.getLogger(IndexBolt.class);
    
    private final Map<SupportedIndexers, String> clastersAddresses;
    private final int cacheSize;
    
    private transient LRUCache<String, Indexer> clients;
    
    public IndexBolt(Map<SupportedIndexers, String> clastersAddresses, int cacheSize) 
    {
        this.clastersAddresses = clastersAddresses;
        this.cacheSize = cacheSize;
    }
    
    @Override
    public void execute(StormTaskTuple t) 
    {
        Indexer indexer = getIndexer(t.getParameter(PluginParameterKeys.INDEXER));
        
        if(indexer == null)
        {
            LOGGER.warn("No indexer. Task {} is dropped.", t.getTaskId());
            emitDropNotification(t.getTaskId(), t.getFileUrl(), "No indexer.", t.getParameters().toString());
            outputCollector.ack(inputTuple);
            return;
        }
        
        String fileWithDataForIndex = t.getFileUrl();
        String rawData = t.getFileByteData();
        String originalFile = t.getParameter(PluginParameterKeys.ORIGINAL_FILE_URL);        
        String fileMetadata = t.getParameter(PluginParameterKeys.FILE_METADATA);    //extracted metadata
        String metadata = t.getParameter(PluginParameterKeys.METADATA);     //additional metadata

        //prepare data
        JsonObject data = new JsonObject();
        if(rawData != null && !rawData.isEmpty())
        {
            data.addProperty(IndexFields.RAW_TEXT.toString(), rawData);
        }
        if(fileMetadata != null && !fileMetadata.isEmpty())
        {
            JsonElement meta = new JsonParser().parse(fileMetadata);
            data.add(IndexFields.FILE_METADATA.toString(), meta);
        }
        if(metadata != null && !metadata.isEmpty())
        {
            JsonObject elements = new JsonParser().parse(metadata).getAsJsonObject();
            
            for (Map.Entry<String,JsonElement> element : elements.entrySet()) 
            {
                data.add(element.getKey(), element.getValue());
            }
        }
    
        try
        {
            //determine what I am indexing
            if(originalFile != null && !originalFile.isEmpty()) 
            {
                //I am indexing extracted data from other file (e.g. features from binary file)
                //If this record already exists, than update fields only
                indexer.update(originalFile, data.toString());
            }
            else if(fileWithDataForIndex != null && !fileWithDataForIndex.isEmpty())
            {
                //I am indexing data from current file (e.g. txt file)
                //If this record already exists, than update fields only
                indexer.update(fileWithDataForIndex, data.toString());
            }
            else
            {
                //I am indexing something other (_id for record will be generated)
                //only create new document - update is not possible
                indexer.insert(data.toString());
            }
        }
        catch(IndexerException ex)
        {
            LOGGER.warn("Cannot index data from tastk {} because: {}", t.getTaskId(), ex.getMessage());
            StringWriter stack = new StringWriter();
            ex.printStackTrace(new PrintWriter(stack));
            emitErrorNotification(t.getTaskId(), t.getFileUrl(), "Cannot index data because: "+ex.getMessage(),
                    stack.toString());
            outputCollector.ack(inputTuple);
            return;
        }
            
        LOGGER.info("Data from task {} is indexed.", t.getTaskId());
                      
        outputCollector.emit(inputTuple, t.toStormTuple());
        outputCollector.ack(inputTuple);
    }

    @Override
    public void prepare() 
    {
        clients = new LRUCache<>(cacheSize);
    }
    
    private Indexer getIndexer(String data)
    {   
        IndexerInformations ii = IndexerInformations.fromTaskString(data);
        
        if(ii == null)
        {
            return null;
        }
        
        String key = ii.toKey();
        if(clients.containsKey(key))
        {
            return clients.get(key);
        }
        
        //key not exists => open new connection and add it to cache
        
        ii.setAddresses(clastersAddresses.get(ii.getIndexerName()));
        
        Indexer client = IndexerFactory.getIndexer(ii);
        clients.put(key, client);
        
        return client;
    }
}
