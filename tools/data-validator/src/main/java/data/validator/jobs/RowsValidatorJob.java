package data.validator.jobs;

import com.datastax.driver.core.*;

import java.util.Date;
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
    private static final int TIME_BETWEEN_RETRIES = 1000; //5 seconds


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
            matchCountWithRetry(row, 0, MAX_RETRY_COUNT);
        }
    }

    private void matchCountWithRetry(Row row, int retryCount, int retryLimit) throws Exception {
        try {
            matchCount(row);
        } catch (Exception e) {
            if (retryCount >= retryLimit-1) {
                throw e;
            }
            Thread.sleep(TIME_BETWEEN_RETRIES);
            matchCountWithRetry(row, ++retryCount, retryLimit);
        }
    }

    private void matchCount(Row row) throws Exception {
        boundTheStatement(row);
        ResultSet resultSet = executeTheQueryWithRetry(0, MAX_RETRY_COUNT);
        long count = resultSet.one().getLong("count");
        if (count != 1) {
            String message = constructTheExceptionMessage(row);
            throw new Exception("The data doesn't fully match!. The exception was thrown for this query: " + matchingBoundStatement.preparedStatement().getQueryString() + " Using these values" + message);
        }
    }

    private void boundTheStatement(Row row) {
        for (int i = 0; i < primaryKeys.size(); i++) {
            boundTheValues(row, i);
        }
    }

    private ResultSet executeTheQueryWithRetry(int retryCount, int retryLimit) throws Exception {
        try {
            ResultSet resultSet = session.execute(matchingBoundStatement);
            return resultSet;
        } catch (Exception e) {
            if (retryCount >= retryLimit-1) {
                throw e;
            }
            Thread.sleep(TIME_BETWEEN_RETRIES);
            return executeTheQueryWithRetry(++retryCount, retryLimit);
        }
    }

    private void boundTheValues(Row row, int i) {
        Object value = row.getObject(i);
        if (value instanceof UUID)
            matchingBoundStatement.setUUID(i, row.getUUID(i));
        else if (value instanceof Date)
            matchingBoundStatement.setDate(i, row.getDate(i));
        else if (value instanceof Integer)
            matchingBoundStatement.setInt(i, row.getInt(i));
        else if (value instanceof Boolean)
            matchingBoundStatement.setBool(i, row.getBool(i));
        else if (value instanceof Long)
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
