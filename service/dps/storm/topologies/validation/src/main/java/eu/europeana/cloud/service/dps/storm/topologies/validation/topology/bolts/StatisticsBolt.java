package eu.europeana.cloud.service.dps.storm.topologies.validation.topology.bolts;

import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.cassandra.CassandraConnectionProviderSingleton;
import eu.europeana.cloud.common.model.dps.RecordState;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.dps.storm.topologies.validation.topology.statistics.RecordStatisticsGenerator;
import eu.europeana.cloud.service.dps.storm.utils.CassandraNodeStatisticsDAO;
import eu.europeana.cloud.service.dps.storm.utils.RecordProcessingStateDAO;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.util.Optional;


public class StatisticsBolt extends AbstractDpsBolt {
    private static final long serialVersionUID = 1L;
    public static final Logger LOGGER = LoggerFactory.getLogger(StatisticsBolt.class);

    private final String hosts;
    private final int port;
    private final String keyspaceName;
    private final String userName;
    private final String password;
    private transient CassandraNodeStatisticsDAO cassandraNodeStatisticsDAO;
    private transient RecordProcessingStateDAO recordProcessingStateDAO;

    public StatisticsBolt(String hosts, int port, String keyspaceName,
                          String userName, String password) {
        this.hosts = hosts;
        this.port = port;
        this.keyspaceName = keyspaceName;
        this.userName = userName;
        this.password = password;
    }

    @Override
    public void prepare() {
        CassandraConnectionProvider cassandraConnectionProvider = CassandraConnectionProviderSingleton.getCassandraConnectionProvider(hosts, port, keyspaceName,
                userName, password);
        cassandraNodeStatisticsDAO = CassandraNodeStatisticsDAO.getInstance(cassandraConnectionProvider);
        recordProcessingStateDAO = RecordProcessingStateDAO.getInstance(cassandraConnectionProvider);
    }

    @Override
    public void execute(Tuple anchorTuple, StormTaskTuple stormTaskTuple) {
        try {
            if (!statsAlreadyCalculated(stormTaskTuple)) {
                LOGGER.info("Calculating file statistics for {}", stormTaskTuple);
                countStatistics(stormTaskTuple);
                markRecordStatsAsCalculated(stormTaskTuple);
            } else {
                LOGGER.info("File stats will NOT be calculated because if was already done in the previous attempt");
            }
            // we can remove the file content before emitting further
            stormTaskTuple.setFileData((byte[]) null);
            outputCollector.emit(anchorTuple, stormTaskTuple.toStormTuple());
        } catch (Exception e) {
            emitErrorNotification(anchorTuple, stormTaskTuple.getTaskId(), stormTaskTuple.getFileUrl(), e.getMessage(), "Statistics for the given file could not be prepared.",
                    Long.parseLong(stormTaskTuple.getParameter(PluginParameterKeys.MESSAGE_PROCESSING_START_TIME_IN_MS)));
        }
        outputCollector.ack(anchorTuple);
    }

    private boolean statsAlreadyCalculated(StormTaskTuple stormTaskTuple) {
        Optional<RecordState> processingRecordStage = recordProcessingStateDAO.selectProcessingRecordStage(stormTaskTuple.getTaskId(), stormTaskTuple.getFileUrl());
        return processingRecordStage.isPresent() && processingRecordStage.get() == RecordState.STATS_GENERATED;
    }

    private void countStatistics(StormTaskTuple stormTaskTuple) throws ParserConfigurationException, SAXException, IOException {
        String document = new String(stormTaskTuple.getFileData());
        RecordStatisticsGenerator statisticsGenerator = new RecordStatisticsGenerator(document);
        cassandraNodeStatisticsDAO.insertNodeStatistics(stormTaskTuple.getTaskId(), statisticsGenerator.getStatistics());
    }

    private void markRecordStatsAsCalculated(StormTaskTuple stormTaskTuple) {
        recordProcessingStateDAO.updateProcessingRecordStage(stormTaskTuple.getTaskId(), stormTaskTuple.getFileUrl(), RecordState.STATS_GENERATED.toString());
    }
}
