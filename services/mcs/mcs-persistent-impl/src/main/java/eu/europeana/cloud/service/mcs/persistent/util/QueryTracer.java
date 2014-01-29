package eu.europeana.cloud.service.mcs.persistent.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ResultSet;

/**
 * Logger for Cassandra queries.
 */
public final class QueryTracer {

    private static final Logger LOGGER = LoggerFactory.getLogger(QueryTracer.class);


    private QueryTracer() {
    }


    /**
     * Logs reuqestet and achived consistency level of Cassandra query.
     * 
     * @param boundStatement
     *            bound statement
     * @param rs
     *            query result set
     */
    public static void logConsistencyLevel(BoundStatement boundStatement, ResultSet rs) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("requested CL {}, achived CL {}", boundStatement.getConsistencyLevel(), rs.getExecutionInfo()
                    .getAchievedConsistencyLevel());
        }
    }
}
