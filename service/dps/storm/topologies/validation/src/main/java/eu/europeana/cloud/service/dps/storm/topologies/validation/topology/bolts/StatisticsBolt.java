package eu.europeana.cloud.service.dps.storm.topologies.validation.topology.bolts;

import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.cassandra.CassandraConnectionProviderSingleton;
import eu.europeana.cloud.common.model.dps.ProcessedRecord;
import eu.europeana.cloud.common.model.dps.RecordState;
import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.dps.storm.service.cassandra.CassandraValidationStatisticsService;
import eu.europeana.cloud.service.dps.storm.topologies.validation.topology.statistics.RecordStatisticsGenerator;
import eu.europeana.cloud.service.dps.storm.utils.ProcessedRecordsDAO;
import eu.europeana.cloud.service.dps.storm.utils.StormTaskTupleHelper;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.util.EnumSet;
import java.util.Optional;


public class StatisticsBolt extends AbstractDpsBolt {
    private static final long serialVersionUID = 1L;
    public static final Logger LOGGER = LoggerFactory.getLogger(StatisticsBolt.class);

    private final String hosts;
    private final int port;
    private final String keyspaceName;
    private final String userName;
    private final String password;
    private transient CassandraValidationStatisticsService statisticsService;
    private transient ProcessedRecordsDAO processedRecordsDAO;

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
        statisticsService = CassandraValidationStatisticsService.getInstance(cassandraConnectionProvider);
        processedRecordsDAO = ProcessedRecordsDAO.getInstance(cassandraConnectionProvider);
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
                    StormTaskTupleHelper.getRecordProcessingStartTime(stormTaskTuple));
        }
        outputCollector.ack(anchorTuple);
    }

    private boolean statsAlreadyCalculated(StormTaskTuple stormTaskTuple) {
        Optional<ProcessedRecord> processingRecordStage = processedRecordsDAO.selectByPrimaryKey(stormTaskTuple.getTaskId(), stormTaskTuple.getFileUrl());
        return processingRecordStage.isPresent() &&
                EnumSet.of(RecordState.STATS_GENERATED, RecordState.ERROR, RecordState.SUCCESS)
                        .contains(processingRecordStage.get().getState());
    }

    private void countStatistics(StormTaskTuple stormTaskTuple) throws ParserConfigurationException, SAXException, IOException {
        String document = new String(stormTaskTuple.getFileData());
        RecordStatisticsGenerator statisticsGenerator = new RecordStatisticsGenerator(document);
        statisticsService.insertNodeStatistics(stormTaskTuple.getTaskId(), statisticsGenerator.getStatistics());
    }

    private void markRecordStatsAsCalculated(StormTaskTuple stormTaskTuple) {
        if (!statsAlreadyCalculated(stormTaskTuple)) {
            processedRecordsDAO.updateProcessedRecordState(stormTaskTuple.getTaskId(), stormTaskTuple.getFileUrl(), RecordState.STATS_GENERATED.toString());
        }
    }
}
