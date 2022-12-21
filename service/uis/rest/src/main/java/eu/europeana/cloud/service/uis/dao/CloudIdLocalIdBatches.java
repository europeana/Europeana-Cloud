package eu.europeana.cloud.service.uis.dao;

import com.datastax.driver.core.BoundStatement;
import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.service.commons.utils.BatchExecutor;
import java.util.ArrayList;

public class CloudIdLocalIdBatches {

  private final CloudIdDAO cloudIdDao;
  private final LocalIdDAO localIdDao;
  private final BatchExecutor batchExecutor;

  public CloudIdLocalIdBatches(CloudIdDAO cloudIdDao, LocalIdDAO localIdDao, CassandraConnectionProvider dbService) {
    this.cloudIdDao = cloudIdDao;
    this.localIdDao = localIdDao;
    this.batchExecutor = BatchExecutor.getInstance(dbService);
  }

  public void insert(String providerId, String recordId, String cloudId) {
    var statementsToBeExecuted = new ArrayList<BoundStatement>();

    statementsToBeExecuted.add(localIdDao.bindInsertStatement(providerId, recordId, cloudId));
    statementsToBeExecuted.add(cloudIdDao.bindInsertStatement(cloudId, providerId, recordId));

    batchExecutor.executeAll(statementsToBeExecuted);
  }
}
