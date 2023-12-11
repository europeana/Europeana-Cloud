package eu.europeana.cloud.service.dps.storm.topologies.oaipmh.bolt;

import eu.europeana.cloud.cassandra.CassandraConnectionProviderSingleton;
import eu.europeana.cloud.common.properties.CassandraProperties;
import eu.europeana.cloud.service.dps.storm.HarvestedRecordCategorizationBolt;
import eu.europeana.cloud.service.dps.storm.dao.HarvestedRecordsDAO;
import eu.europeana.cloud.service.dps.storm.topologies.oaipmh.utils.OaiPmhTopologyCategorizationService;

/**
 * Bolt that will categorize record in the incremental harvesting if it should be processed or not
 */
public class OaiHarvestedRecordCategorizationBolt extends HarvestedRecordCategorizationBolt {

  public OaiHarvestedRecordCategorizationBolt(CassandraProperties cassandraProperties) {
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

    harvestedRecordCategorizationService = new OaiPmhTopologyCategorizationService(
        HarvestedRecordsDAO.getInstance(cassandraConnectionProvider));
  }
}
