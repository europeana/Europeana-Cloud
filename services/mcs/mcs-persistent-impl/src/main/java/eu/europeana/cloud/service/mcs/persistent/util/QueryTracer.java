package eu.europeana.cloud.service.mcs.persistent.util;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ResultSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryTracer {

    private final static Logger logger = LoggerFactory.getLogger(QueryTracer.class);


    public static void logConsistencyLevel(BoundStatement boundStatement, ResultSet rs) {
        if (logger.isDebugEnabled()) {
            logger.debug("requested CL {}, achived CL {}", boundStatement.getConsistencyLevel(), rs.getExecutionInfo()
                    .getAchievedConsistencyLevel());
        }
    }
}
