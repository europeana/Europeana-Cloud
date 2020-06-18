package eu.europeana.cloud.service.dps.depublish;

import eu.europeana.indexing.Indexer;
import eu.europeana.indexing.exception.IndexingException;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;

import static org.junit.Assert.*;

@Ignore
public class MetisIndexerFactoryItest {

    private MetisIndexerFactory factory;

    @Before
    public void prepare() throws IOException {
        factory = new MetisIndexerFactory();
    }

    @Test
    public void testOpenIndexerReturnsValidCountOfSet109() throws IOException, IndexingException, URISyntaxException {
        try (Indexer indexer = factory.openIndexer(false)) {
            long count = indexer.countRecords("109");
            assertEquals(1614L, count);
        }
    }

    @Test
    public void testOpenIndexerAlternativeEnvironmentReturnsValidCountOfSet109() throws IOException, IndexingException, URISyntaxException {
        try (Indexer indexer = factory.openIndexer(true)) {
            long count = indexer.countRecords("109");
            assertEquals(401L, count);
        }
    }

    @Test
    public void testBigSetCount() throws IOException, IndexingException, URISyntaxException {
        try (Indexer indexer = factory.openIndexer(true)) {
            long count = indexer.countRecords("165");
            assertEquals(219947L, count);
        }
    }

    @Test
    public void testSmallSetCountVerify11Elements() throws IOException, IndexingException, URISyntaxException {
        try (Indexer indexer = factory.openIndexer(true)) {
            long count = indexer.countRecords("198");
            assertEquals(11L, count);
        }
    }

    @Test
    public void testFin() throws IOException, IndexingException, URISyntaxException {
        try (Indexer indexer = factory.openIndexer(true)) {
            long count = indexer.countRecords("194");
            assertEquals(80889L, count);
        }
    }
}