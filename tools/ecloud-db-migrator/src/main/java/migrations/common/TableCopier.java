package migrations.common;

import java.util.Iterator;
import com.datastax.driver.core.*;
import eu.europeana.cloud.service.commons.utils.RetryInterruptedException;

/**
 * @author krystian.
 */
public abstract class TableCopier {

    private final static int DEFAULT_RETRIES = 20;

    private final static long SLEEP_TIME = 1000;

    public void copyTable(Session session, PreparedStatement selectStatement, PreparedStatement insertStatement) {
        insertStatement.setConsistencyLevel(ConsistencyLevel.QUORUM);
        selectStatement.setConsistencyLevel(ConsistencyLevel.QUORUM);

        long counter = 0;

        BoundStatement boundStatement = selectStatement.bind();
        boundStatement.setFetchSize(100);
        ResultSet rs = session.execute(boundStatement);

        Iterator<Row> ri = rs.iterator();

        while (hasNextRow(ri)) {
            Row r = ri.next();
            insert(insertStatement, r, session);
            if (++counter % 10000 == 0) {
                System.out.print("\rCopy table progress: " + counter);
            }
        }
        if (counter > 0) {
            System.out.println("\rCopy table progress: " + counter);
        }
    }

    public static boolean hasNextRow(Iterator<Row> iterator) {
        int retries = DEFAULT_RETRIES;

        while (retries-- > 0) {
            try {
                return iterator.hasNext();
            } catch (Exception e) {
                if (retries > 0){
                    try {
                        System.out.println("Sleeping");
                        Thread.sleep(SLEEP_TIME * (DEFAULT_RETRIES - retries));
                    } catch (InterruptedException e1) {
                        Thread.currentThread().interrupt();
                        throw new RetryInterruptedException(e);
                    }
                } else {
                    System.out.println("Exception while copying table.\n" + e.getMessage());
                    throw e;
                }
            }
        }
        return false;
    }


    public abstract void insert(PreparedStatement insertStatement, Row r, Session session) ;
}
