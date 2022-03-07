package eu.europeana.cloud.service.dps.metis.indexing;

import com.google.common.base.Charsets;
import eu.europeana.indexing.IndexingProperties;
import eu.europeana.indexing.exception.IndexingException;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static eu.europeana.cloud.service.dps.metis.indexing.DatabaseLocation.DEFAULT_PREVIEW;

public class ParallelIndexerTester {

    private static String DIRECTORY="./service/dps/rest/src/test/resources/files";

    public static void main(String[] args) throws IndexingException, IOException, ExecutionException, InterruptedException {
        TestIndexWrapper wrapper = new TestIndexWrapper();
        try {
            indexFileInManyThreads(wrapper);
        } finally {
            wrapper.close();
        }

    }

    private static void indexFileInManyThreads(TestIndexWrapper wrapper) throws IOException, InterruptedException, ExecutionException {
        Date indexDate=new Date();


        ExecutorService executor = Executors.newFixedThreadPool(20);

        IndexingProperties indexingProperties=new IndexingProperties(indexDate,false,
                null,false,true);

        List<Future> results=new ArrayList<>();

        for (File file : new File(DIRECTORY).listFiles()) {
            System.out.println("Read file: " + file);

            String record = FileUtils.readFileToString(file, Charsets.UTF_8);

            results.add(executor.submit(() -> {
                        System.out.println("Indexing: " + file);
                        wrapper.indexers.get(DEFAULT_PREVIEW).index(record, indexingProperties);
                        return null;
                    }
            ));
        }

        System.out.println("Finished. Checking results......");
        for(Future<Void> result:results) {
            result.get();
            System.out.println("Partial result OK.");
        }
        System.out.println("Verified. All OK.");
    }


}
