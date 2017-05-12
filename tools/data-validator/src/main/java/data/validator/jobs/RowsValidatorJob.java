package data.validator.jobs;

import com.datastax.driver.core.*;
import data.validator.constants.CassandraColumnTypes;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;

/**
 * Created by Tarek on 4/28/2017.
 */

public class RowsValidatorJob implements Callable<Void> {
    private Session session;
    private List<String> primaryKeys;
    private BoundStatement matchingBoundStatement;
    private List<Row> rows;
    private static final int MAX_RETRY_COUNT = 3;
    private static final int TIME_BETWEEN_RETRIES = 1000; //One second


    public RowsValidatorJob(Session session, List<String> primaryKeys, BoundStatement matchingBoundStatement, List<Row> rows) {
        this.session = session;
        this.primaryKeys = primaryKeys;
        this.matchingBoundStatement = matchingBoundStatement;
        this.rows = rows;
    }

    @Override
    public Void call() throws Exception {
        validateRows();
        return null;
    }


    private void validateRows() throws Exception {
        for (Row row : rows) {
            getMatchingCountWithRetry(row, 0, MAX_RETRY_COUNT);
        }
    }

    private long getMatchingCountWithRetry(Row row, int retryCount, int retryLimit) throws Exception {
        try {
            return getMatchingCount(row);
        } catch (Exception e) {
            if (retryCount > retryLimit) {
                throw e;
            }
            Thread.sleep(TIME_BETWEEN_RETRIES);
            return getMatchingCountWithRetry(row, ++retryCount, retryLimit);
        }
    }

    private long getMatchingCount(Row row) throws Exception {
        for (int i = 0; i < primaryKeys.size(); i++) {
            boundTheValues(row, i);
        }
        ResultSet resultSet = session.execute(matchingBoundStatement);
        long count = resultSet.one().getLong("count");
        if (count != 1) {
            String message = constructTheExceptionMessage(row);
            throw new Exception("The data doesn't fully match!. The exception was thrown for this query: " + matchingBoundStatement.preparedStatement().getQueryString() + " Using these values" + message);
        }
        return count;
    }

    private void boundTheValues(Row row, int i) {
        Object value = row.getObject(i);
        if (value.getClass().getName().equals(CassandraColumnTypes.UUID.getClassName()))
            matchingBoundStatement.setUUID(i, row.getUUID(i));
        else if (value.getClass().getName().equals(CassandraColumnTypes.DATE.getClassName()))
            matchingBoundStatement.setDate(i, row.getDate(i));
        else if (value.getClass().getName().equals(CassandraColumnTypes.INTEGER.getClassName()))
            matchingBoundStatement.setInt(i, row.getInt(i));
        else if (value.getClass().getName().equals(CassandraColumnTypes.BOOLEAN.getClassName()))
            matchingBoundStatement.setBool(i, row.getBool(i));
        else if (value.getClass().getName().equals(CassandraColumnTypes.LONG.getClassName()))
            matchingBoundStatement.setLong(i, row.getLong(i));
        else matchingBoundStatement.setString(i, value.toString());
    }

    private String constructTheExceptionMessage(Row row) {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("(");
        for (int i = 0; i < primaryKeys.size() - 1; i++)
            stringBuilder.append(row.getObject(i)).append(",");
        stringBuilder.append(row.getObject(primaryKeys.size() - 1)).append(")");
        return stringBuilder.toString();
    }
}
