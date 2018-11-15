package data.validator.jobs;

import com.datastax.driver.core.*;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.when;

/**
 * Created by Tarek on 5/16/2017.
 */
public class RowsValidatorJobTest {
    private static Session session;
    private static List<String> primaryKeys;
    private static BoundStatement matchingBoundStatement;
    private static List<Row> rows;

    @BeforeClass
    public static void init() {
        primaryKeys = Arrays.asList("primaryKey");
        Row row1 = mock(Row.class);
        when(row1.getObject(0)).thenReturn("Item1");
        rows = Arrays.asList(row1);
        when(row1.getObject(anyInt())).thenReturn("Item1");
        matchingBoundStatement = mock(BoundStatement.class);
        session = mock(Session.class);

    }

    @Test
    public void shouldExecuteTheSessionWithoutRetry() throws Exception {

        ResultSet resultSet = mock(ResultSet.class);
        when(resultSet.one()).thenReturn(rows.get(0));
        when(resultSet.one().getLong("count")).thenReturn(1l);


        when(session.execute(matchingBoundStatement)).thenReturn(resultSet);
        PreparedStatement preparedStatement = mock(PreparedStatement.class);

        when(matchingBoundStatement.preparedStatement()).thenReturn(preparedStatement);
        when(preparedStatement.getQueryString()).thenReturn("Query String");

        final RowsValidatorJob job = new RowsValidatorJob(session, primaryKeys, matchingBoundStatement, rows);
        job.call();
        verify(session, times(1)).execute(any(BoundStatement.class));
    }

    @Test
    public void shouldExecuteTheSessionWithMaximumRetry() throws Exception {
        ResultSet resultSet = mock(ResultSet.class);
        when(resultSet.one()).thenReturn(rows.get(0));
        when(resultSet.one().getLong("count")).thenReturn(0l);

        final Session session = mock(Session.class);
        when(session.execute(matchingBoundStatement)).thenReturn(resultSet);


        final RowsValidatorJob job = new RowsValidatorJob(session, primaryKeys, matchingBoundStatement, rows);
        try {
            job.call();
            assertTrue(false);
        } catch (Exception e) {
            //an Exception should be thrown
        }
        verify(session, times(5)).execute(any(BoundStatement.class));
    }

    @Test
    public void shouldThrowAnExceptionWithCorrectMessage() throws Exception {
        ResultSet resultSet = mock(ResultSet.class);
        when(resultSet.one()).thenReturn(rows.get(0));
        when(resultSet.one().getLong("count")).thenReturn(0l);

        final Session session = mock(Session.class);
        when(session.execute(matchingBoundStatement)).thenReturn(resultSet);

        PreparedStatement preparedStatement = mock(PreparedStatement.class);

        when(matchingBoundStatement.preparedStatement()).thenReturn(preparedStatement);
        when(preparedStatement.getQueryString()).thenReturn("Select count(*) From TableName");


        final RowsValidatorJob job = new RowsValidatorJob(session, primaryKeys, matchingBoundStatement, rows);
        try {
            job.call();
        } catch (Exception e) {
            assertEquals("The data doesn't fully match!. The exception was thrown for this query: Select count(*) From TableName Using these values(Item1)", e.getMessage());
        }
        verify(session, times(5)).execute(any(BoundStatement.class));
    }
}

