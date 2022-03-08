package eu.europeana.cloud.service.dps.depublish;

import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.service.utils.indexing.IndexWrapper;
import eu.europeana.cloud.service.dps.metis.indexing.TargetIndexingDatabase;
import eu.europeana.cloud.service.dps.storm.utils.SubmitTaskParameters;
import eu.europeana.indexing.Indexer;
import eu.europeana.indexing.exception.IndexingException;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

@Service
public class DatasetDepublisher {

    private final Indexer indexer;

    public DatasetDepublisher(IndexWrapper indexWrapper) {
        this.indexer = indexWrapper.getIndexer(TargetIndexingDatabase.PUBLISH);
    }

    @Async
    public Future<Integer> executeDatasetDepublicationAsync(SubmitTaskParameters parameters) throws IndexingException {
        int removedCount = indexer.removeAll(parameters.getTaskParameter(PluginParameterKeys.METIS_DATASET_ID), null);
        return CompletableFuture.completedFuture(removedCount);
    }

    public boolean removeRecord(String recordId) throws IndexingException {
        return indexer.remove(recordId);
    }

    public long getRecordsCount(SubmitTaskParameters parameters) throws IndexingException, URISyntaxException, IOException {
        return indexer.countRecords(parameters.getTaskParameter(PluginParameterKeys.METIS_DATASET_ID));
    }

}
