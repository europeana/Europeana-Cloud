package eu.europeana.cloud.service.dps.storm.topologies.validation.topology.bolts;

import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.cassandra.CassandraConnectionProviderSingleton;
import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.dps.storm.topologies.validation.topology.statistics.RecordStatisticsGenerator;
import eu.europeana.cloud.service.dps.storm.utils.CassandraNodeStatisticsDAO;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;


public class StatisticsBolt extends AbstractDpsBolt {
    private String hosts;
    private int port;
    private String keyspaceName;
    private String userName;
    private String password;
    private static CassandraConnectionProvider cassandraConnectionProvider;
    private static CassandraNodeStatisticsDAO cassandraNodeStatisticsDAO;

    public StatisticsBolt(String hosts, int port, String keyspaceName,
                          String userName, String password) {
        this.hosts = hosts;
        this.port = port;
        this.keyspaceName = keyspaceName;
        this.userName = userName;
        this.password = password;
    }

    @Override
    public void execute(StormTaskTuple stormTaskTuple) {
        try {
            countStatistics(stormTaskTuple);
            // we can remove the file content before emitting further
            stormTaskTuple.setFileData((byte[]) null);
            outputCollector.emit(inputTuple, stormTaskTuple.toStormTuple());
        } catch (Exception e) {
            emitErrorNotification(stormTaskTuple.getTaskId(), stormTaskTuple.getFileUrl(), e.getMessage(), "Statistics for the given file could not be prepared.");
        }
    }

    @Override
    public void prepare() {
        cassandraConnectionProvider = CassandraConnectionProviderSingleton.getCassandraConnectionProvider(hosts, port, keyspaceName,
                userName, password);
        cassandraNodeStatisticsDAO = CassandraNodeStatisticsDAO.getInstance(cassandraConnectionProvider);
    }

    private void countStatistics(StormTaskTuple stormTaskTuple) throws ParserConfigurationException, SAXException, IOException {
        String document = new String(stormTaskTuple.getFileData());
        RecordStatisticsGenerator statisticsGenerator = new RecordStatisticsGenerator(document);
        cassandraNodeStatisticsDAO.insertNodeStatistics(stormTaskTuple.getTaskId(), statisticsGenerator.getStatistics());
    }
}
