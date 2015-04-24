package eu.europeana.cloud.service.dps.storm.topologies.indexer;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import java.util.Map;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Pavel Kefurt <Pavel.Kefurt@gmail.com>
 */
public class IndexBolt extends AbstractDpsBolt
{
    private static final Logger LOGGER = LoggerFactory.getLogger(IndexBolt.class);
    
    private final String clasterAddresses;
    
    private transient Client client;
    
    public IndexBolt(String clasterAddresses) 
    {
        this.clasterAddresses = clasterAddresses;
    }
    
    @Override
    public void execute(StormTaskTuple t) 
    {
        String fileWithDataForIndex = t.getFileUrl();
        String rawData = t.getFileByteData();
        String originalFile = t.getParameter(PluginParameterKeys.ORIGINAL_FILE_URL);        
        String fileMetadata = t.getParameter(PluginParameterKeys.FILE_METADATA);    //extracted metadata
        String metadata = t.getParameter(PluginParameterKeys.METADATA);     //additional metadata
        String index = t.getParameter(PluginParameterKeys.ELASTICSEARCH_INDEX);
        String type = t.getParameter(PluginParameterKeys.ELASTICSEARCH_TYPE);

        //prepare data
        JsonObject data = new JsonObject();
        if(rawData != null && !rawData.isEmpty())
        {
            data.addProperty(IndexerConstants.RAW_DATA_FIELD, rawData);
        }
        if(fileMetadata != null && !fileMetadata.isEmpty())
        {
            JsonElement meta = new JsonParser().parse(fileMetadata);
            data.add(IndexerConstants.FILE_METADATA_FIELD, meta);
        }
        if(metadata != null && !metadata.isEmpty())
        {
            JsonObject elements = new JsonParser().parse(metadata).getAsJsonObject();
            
            for (Map.Entry<String,JsonElement> element : elements.entrySet()) 
            {
                data.add(element.getKey(), element.getValue());
            }
        }
        
        UpdateResponse updateResponse = null;
        IndexResponse indexResponse = null;
    
        //determine what I am indexing
        if(originalFile != null && !originalFile.isEmpty()) 
        {
            //I am indexing extracted data from other file (e.g. features from binary file)
            //If this record already exists, than update fields only
            UpdateRequestBuilder prepareUpdate = client.prepareUpdate(index, type, originalFile).setDocAsUpsert(true);
            updateResponse = prepareUpdate.setDoc(data.toString()).execute().actionGet();
        }
        else if(fileWithDataForIndex != null && !fileWithDataForIndex.isEmpty())
        {
            //I am indexing data from current file (e.g. txt file)
            //If this record already exists, than update fields only
            UpdateRequestBuilder prepareUpdate = client.prepareUpdate(index, type, fileWithDataForIndex).setDocAsUpsert(true);
            updateResponse = prepareUpdate.setDoc(data.toString()).execute().actionGet();
        }
        else
        {
            //I am indexing something other (_id for record will be generated)
            //only create new document - update is not possible
            IndexRequestBuilder prepareIndex = client.prepareIndex(index, type);
            indexResponse = prepareIndex.setSource(data.toString()).execute().actionGet();
        }
       
        if(indexResponse != null)
        {
           LOGGER.info("Created new index '{}/{}/{}' with data '{}'",
                   indexResponse.getIndex(), indexResponse.getType() , indexResponse.getId(), data.toString()); 
        }
        else
        {
           assert updateResponse != null;
           LOGGER.info("Updated index '{}/{}/{}' with data '{}'", 
                   updateResponse.getIndex(), updateResponse.getType() , updateResponse.getId(), data.toString()); 
        }        
        
        outputCollector.emit(inputTuple, t.toStormTuple());
        outputCollector.ack(inputTuple);
    }

    @Override
    public void prepare() 
    {
        String[] addresses = clasterAddresses.split(";");
        
        TransportClient transportClient = new TransportClient();
        
        for(String address: addresses)
        {
            if(address != null && !address.isEmpty())
            {
                String[] split = address.split(":");
                if(split.length == 2)
                {
                    transportClient.addTransportAddress(new InetSocketTransportAddress(split[0], Integer.parseInt(split[1])));
                }
                else
                {
                    LOGGER.warn("Can not parse address '{}' because: Bad address format", address);
                }
            }
        }
        
        client = transportClient;
    }
}
