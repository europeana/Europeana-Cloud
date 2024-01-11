package eu.europeana.cloud.http.bolts;

import eu.europeana.cloud.cassandra.CassandraConnectionProviderSingleton;
import eu.europeana.cloud.common.properties.CassandraProperties;
import eu.europeana.cloud.http.service.HttpTopologyCategorizationService;
import eu.europeana.cloud.service.dps.storm.HarvestedRecordCategorizationBolt;
import eu.europeana.cloud.service.dps.storm.dao.HarvestedRecordsDAO;

public class HttpHarvestedRecordCategorizationBolt extends HarvestedRecordCategorizationBolt {

  public HttpHarvestedRecordCategorizationBolt(CassandraProperties cassandraProperties) {
    super(cassandraProperties);
  }

  @Override
  public void prepare() {
    var cassandraConnectionProvider =
        CassandraConnectionProviderSingleton.getCassandraConnectionProvider(
            cassandraProperties.getHosts(),
            cassandraProperties.getPort(),
            cassandraProperties.getKeyspace(),
            cassandraProperties.getUser(),
            cassandraProperties.getPassword());

    harvestedRecordCategorizationService = new HttpTopologyCategorizationService(
        HarvestedRecordsDAO.getInstance(cassandraConnectionProvider));
  }
}
