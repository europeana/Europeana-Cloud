package data.validator.cql;

import com.datastax.driver.core.*;
import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static data.validator.constants.Constants.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Created by Tarek on 5/17/2017.
 */
public class CassandraHelperTest {

  private static List<String> primaryKeys;
  private static CassandraConnectionProvider cassandraConnectionProvider;
  private static Session session;
  private static PreparedStatement preparedStatement;
  private static BoundStatement boundStatement;


  @BeforeClass
  public static void init() {
    primaryKeys = Arrays.asList("key1", "key2");
    cassandraConnectionProvider = mock(CassandraConnectionProvider.class);
    session = mock(Session.class);
    when(cassandraConnectionProvider.getSession()).thenReturn(session);
    preparedStatement = mock(PreparedStatement.class);
    when(session.prepare(anyString())).thenReturn(preparedStatement);
    when(preparedStatement.setConsistencyLevel(any(ConsistencyLevel.class))).thenReturn(preparedStatement);
    boundStatement = mock(BoundStatement.class);
    when(preparedStatement.bind()).thenReturn(boundStatement);

  }

  @Test
  public void prepareBoundStatementForMatchingTargetTableTest() {
    assertThat(CassandraHelper.prepareBoundStatementForMatchingTargetTable(cassandraConnectionProvider, "table", primaryKeys),
        is(boundStatement));
  }


  @Test
  public void getPrimaryKeysFromSourceTableTest() {
    ResultSet resultSet = mock(ResultSet.class);
    when(session.execute(boundStatement)).thenReturn(resultSet);
    assertThat(CassandraHelper.getPrimaryKeysFromSourceTable(cassandraConnectionProvider, "table", primaryKeys), is(resultSet));
  }

  @Test
  public void getPrimaryKeysNamesTest() {
    ResultSet resultSet = mock(ResultSet.class);
    when(preparedStatement.bind(anyString(), anyString())).thenReturn(boundStatement);
    when(session.execute(boundStatement)).thenReturn(resultSet);
    Iterator iterator = mock(Iterator.class);
    when(resultSet.iterator()).thenReturn(iterator);
    when(iterator.hasNext()).thenReturn(true).thenReturn(true).thenReturn(false);
    Row row1 = mock(Row.class);
    Row row2 = mock(Row.class);
    when(iterator.next()).thenReturn(row1).thenReturn(row2);
    when(row1.getString(COLUMN_INDEX_TYPE)).thenReturn(CLUSTERING_KEY_TYPE);
    when(row2.getString(COLUMN_INDEX_TYPE)).thenReturn(CLUSTERING_KEY_TYPE);
    when(row1.getString(COLUMN_INDEX_TYPE)).thenReturn(PARTITION_KEY_TYPE);
    when(row2.getString(COLUMN_INDEX_TYPE)).thenReturn(PARTITION_KEY_TYPE);
    when(row1.getString(COLUMN_NAME_SELECTOR)).thenReturn(primaryKeys.get(0));
    when(row2.getString(COLUMN_NAME_SELECTOR)).thenReturn(primaryKeys.get(1));
    assertThat(CassandraHelper.getPrimaryKeysNames(cassandraConnectionProvider, "table", "Query String"), is(primaryKeys));
  }

}


