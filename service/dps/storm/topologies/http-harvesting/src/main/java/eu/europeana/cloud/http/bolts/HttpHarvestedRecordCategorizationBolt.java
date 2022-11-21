package eu.europeana.cloud.http.bolts;

import eu.europeana.cloud.cassandra.CassandraConnectionProviderSingleton;
import eu.europeana.cloud.http.service.HttpTopologyCategorizationService;
import eu.europeana.cloud.service.dps.storm.HarvestedRecordCategorizationBolt;
import eu.europeana.cloud.service.dps.storm.dao.HarvestedRecordsDAO;
import eu.europeana.cloud.service.dps.storm.utils.DbConnectionDetails;

public class HttpHarvestedRecordCategorizationBolt extends HarvestedRecordCategorizationBolt {

  private final DbConnectionDetails dbConnectionDetails;

  public HttpHarvestedRecordCategorizationBolt(DbConnectionDetails dbConnectionDetails) {
    this.dbConnectionDetails = dbConnectionDetails;
  }

  @Override
  public void prepare() {
    var cassandraConnectionProvider =
        CassandraConnectionProviderSingleton.getCassandraConnectionProvider(
            dbConnectionDetails.getHosts(),
            dbConnectionDetails.getPort(),
            dbConnectionDetails.getKeyspaceName(),
            dbConnectionDetails.getUserName(),
            dbConnectionDetails.getPassword());

    harvestedRecordCategorizationService = new HttpTopologyCategorizationService(
        HarvestedRecordsDAO.getInstance(cassandraConnectionProvider));
  }
}
