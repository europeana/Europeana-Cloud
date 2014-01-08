package eu.europeana.cloud.service.mcs.persistent;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ResultSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryTracer {

    private final static Logger logger = LoggerFactory.getLogger(QueryTracer.class);


    static void logConsistencyLevel(BoundStatement boundStatement, ResultSet rs) {
        logger.debug("requested CL {}, achived CL {}", boundStatement.getConsistencyLevel(), rs.getExecutionInfo()
                .getAchievedConsistencyLevel());
    }
}
