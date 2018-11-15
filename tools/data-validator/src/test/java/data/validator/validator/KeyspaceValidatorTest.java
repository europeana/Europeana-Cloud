package data.validator.validator;

import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.TableMetadata;
import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;

/**
 * Created by Tarek on 5/17/2017.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(KeyspaceValidator.class)
public class KeyspaceValidatorTest {
    @Test
    public void verifyJobExecution() throws Exception {
        CassandraConnectionProvider cassandraConnectionProvider = mock(CassandraConnectionProvider.class);
        int threadCount = 1;
        String tableName = "tableName";
        KeyspaceValidator keyspaceValidator = new KeyspaceValidator();
        Metadata metadata = mock(Metadata.class);
        KeyspaceMetadata keyspaceMetadata = mock(KeyspaceMetadata.class);
        when(cassandraConnectionProvider.getMetadata()).thenReturn(metadata);
        when(cassandraConnectionProvider.getKeyspaceName()).thenReturn("keyspace");
        when(metadata.getKeyspace(anyString())).thenReturn(keyspaceMetadata);
        TableMetadata tableMetadata = mock(TableMetadata.class);
        when(tableMetadata.getName()).thenReturn(tableName);
        Collection<TableMetadata> tableMetadataList = Arrays.asList(tableMetadata);
        when(keyspaceMetadata.getTables()).thenReturn(tableMetadataList);
        PowerMockito.whenNew(CassandraConnectionProvider.class).withAnyArguments().thenReturn(cassandraConnectionProvider);
        ExecutorService executorService = mock(ExecutorService.class);
        PowerMockito.mockStatic(Executors.class);
        when(Executors.newFixedThreadPool(threadCount)).thenReturn(executorService);
        keyspaceValidator.validate(cassandraConnectionProvider, cassandraConnectionProvider, tableName, tableName, threadCount);
        verify(executorService, times(1)).invokeAll(any(Collection.class));


    }
}
