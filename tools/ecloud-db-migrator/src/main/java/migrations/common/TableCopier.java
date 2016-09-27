package migrations.common;

import com.datastax.driver.core.*;

/**
 * @author krystian.
 */
public abstract class TableCopier {

    public void copyTable(Session session, PreparedStatement selectStatement, PreparedStatement insertStatement) {
        insertStatement.setConsistencyLevel(ConsistencyLevel.QUORUM);
        selectStatement.setConsistencyLevel(ConsistencyLevel.QUORUM);

        BoundStatement boundStatement = selectStatement.bind();
        boundStatement.setFetchSize(100);
        ResultSet rs = session.execute(boundStatement);
        for (Row r : rs){
            insert(insertStatement, r,session);
        }
    }

    public abstract void insert(PreparedStatement insertStatement, Row r, Session session) ;
}
