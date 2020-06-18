package eu.europeana.cloud.service.dps.depublish;

import eu.europeana.indexing.Indexer;
import eu.europeana.indexing.exception.IndexingException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

@Service
public class DatasetDepublisher {

    @Autowired
    private MetisIndexerFactory indexerFactory;

    @Async
    public Future<Integer> executeDatasetDepublicationAsync(String datasetMetisId, boolean alternativeEnvironment) throws IndexingException, URISyntaxException, IOException {
        try (Indexer indexer = indexerFactory.openIndexer(alternativeEnvironment)) {
            int removedCount = indexer.removeAll(datasetMetisId, null);
            return CompletableFuture.completedFuture(removedCount);
        }
    }

    public long getRecordsCount(String datasetMetisId, boolean alternativeEnvironment) throws IndexingException, URISyntaxException, IOException {
        try (Indexer indexer = indexerFactory.openIndexer(alternativeEnvironment)) {
            return indexer.countRecords(datasetMetisId);
        }
    }

}
