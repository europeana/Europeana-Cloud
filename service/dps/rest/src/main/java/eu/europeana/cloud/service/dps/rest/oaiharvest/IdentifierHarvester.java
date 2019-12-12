package eu.europeana.cloud.service.dps.rest.oaiharvest;

import eu.europeana.cloud.service.dps.RecordExecutionSubmitService;
import eu.europeana.cloud.service.dps.storm.utils.CassandraTaskInfoDAO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

public class IdentifierHarvester {
    private static final Logger LOGGER = LoggerFactory.getLogger(IdentifierHarvester_old.class);

    @Autowired
    private RecordExecutionSubmitService recordSubmitService;

    @Autowired
    private CassandraTaskInfoDAO taskInfoDAO;


    public void harvest(String topologyName, String identifier) {
        LOGGER.info("harvest("+topologyName+", "+identifier+")");
    }
}
