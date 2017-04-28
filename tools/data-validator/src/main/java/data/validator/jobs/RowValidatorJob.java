package data.validator.jobs;

import com.datastax.driver.core.*;

import java.util.List;
import java.util.concurrent.Callable;

/**
 * Created by Tarek on 4/28/2017.
 */

public class RowValidatorJob implements Callable<Long> {
    private Session session;
    private List<String> primaryKeys;
    private BoundStatement matchingBoundStatement;
    private Row row;
    private static final int MAX_RETRY_COUNT = 3;
    private static final int TIME_BETWEEN_RETRIES = 1000; //One second


    public RowValidatorJob(Session session, List<String> primaryKeys, BoundStatement matchingBoundStatement, Row row) {
        this.session = session;
        this.primaryKeys = primaryKeys;
        this.matchingBoundStatement = matchingBoundStatement;
        this.row = row;
    }

    @Override
    public Long call() throws Exception {
        long count = getMatchingCountWithRetry(0, MAX_RETRY_COUNT);
        return count;
    }


    private long getMatchingCountWithRetry(int retryCount, int retryLimit) throws Exception {
        try {
            return getMatchingCount();
        } catch (Exception e) {
            if (retryCount > retryLimit) {
                throw e;
            }
            Thread.sleep(TIME_BETWEEN_RETRIES);
            return getMatchingCountWithRetry(++retryCount, retryLimit);
        }
    }

    private long getMatchingCount() throws Exception {
        for (int i = 0; i < primaryKeys.size(); i++) {
            Object value = row.getObject(i);
            matchingBoundStatement.setString(i, value.toString());
        }
        ResultSet resultSet = session.execute(matchingBoundStatement);
        long count = resultSet.one().getLong("count");
        if (count != 1) {
            String message = constructTheExceptionMessage();
            throw new Exception("The data doesn't fully match!. The exception was thrown for this query: " + matchingBoundStatement.preparedStatement().getQueryString() + " Using these values" + message);
        }
        return count;
    }

    private String constructTheExceptionMessage() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("(");
        for (int i = 0; i < primaryKeys.size() - 1; i++)
            stringBuilder.append(row.getObject(i)).append(",");
        stringBuilder.append(row.getObject(primaryKeys.size() - 1)).append(")");
        return stringBuilder.toString();
    }
}
