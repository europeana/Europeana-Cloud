package eu.europeana.cloud.service.dps.storm.topologies.indexing.bolts;

import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.indexing.*;

import java.io.IOException;

/**
 * Created by pwozniak on 4/6/18
 */
public class IndexingBolt extends AbstractDpsBolt {


    private IndexerFactory indexerFactory;

    public IndexingBolt(IndexerFactory indexingFactory) {
        this.indexerFactory = indexingFactory;
    }

    @Override
    public void prepare() {
    }

    @Override
    public void execute(StormTaskTuple stormTaskTuple) {
        try(final Indexer indexer = indexerFactory.getIndexer()){
            String document = new String(stormTaskTuple.getFileData());
            indexer.index(document);
            outputCollector.emit(inputTuple, stormTaskTuple.toStormTuple());
        }catch(IndexerConfigurationException e){
            emitErrorNotification(stormTaskTuple.getTaskId(), stormTaskTuple.getFileUrl(), e.getMessage(), "Error in indexer configuration");
        } catch (IOException e) {
            emitErrorNotification(stormTaskTuple.getTaskId(), stormTaskTuple.getFileUrl(), e.getMessage(), "Error while retrieving indexer");
        } catch (IndexingException e) {
            emitErrorNotification(stormTaskTuple.getTaskId(), stormTaskTuple.getFileUrl(), e.getMessage(), "Error while indexing");
        }
    }
}
