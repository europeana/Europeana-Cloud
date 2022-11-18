package eu.europeana.cloud.service.dps.storm.topologies.oaipmh.bolt;

import eu.europeana.cloud.cassandra.CassandraConnectionProviderSingleton;
import eu.europeana.cloud.service.dps.storm.HarvestedRecordCategorizationBolt;
import eu.europeana.cloud.service.dps.storm.dao.HarvestedRecordsDAO;
import eu.europeana.cloud.service.dps.storm.topologies.oaipmh.utils.OaiPmhTopologyCategorizationService;
import eu.europeana.cloud.service.dps.storm.utils.DbConnectionDetails;

/**
 * Bolt that will categorize record in the incremental harvesting if it should be processed or not
 */
public class OaiHarvestedRecordCategorizationBolt extends HarvestedRecordCategorizationBolt {

    private final DbConnectionDetails dbConnectionDetails;

    public OaiHarvestedRecordCategorizationBolt(DbConnectionDetails dbConnectionDetails) {
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

        harvestedRecordCategorizationService = new OaiPmhTopologyCategorizationService(HarvestedRecordsDAO.getInstance(cassandraConnectionProvider));
    }
}
