package eu.europeana.cloud.service.dps.storm.topologies.validation.topology.bolts;

import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.cassandra.CassandraConnectionProviderSingleton;
import eu.europeana.cloud.common.model.dps.ProcessedRecord;
import eu.europeana.cloud.common.model.dps.RecordState;
import eu.europeana.cloud.common.properties.CassandraProperties;
import eu.europeana.cloud.service.commons.utils.RetryInterruptedException;
import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.dps.storm.dao.ProcessedRecordsDAO;
import eu.europeana.cloud.service.dps.storm.service.ValidationStatisticsServiceImpl;
import eu.europeana.cloud.service.dps.storm.topologies.validation.topology.statistics.RecordStatisticsGenerator;
import eu.europeana.cloud.service.dps.storm.utils.StormTaskTupleHelper;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.EnumSet;
import java.util.Optional;
import javax.xml.parsers.ParserConfigurationException;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;


public class StatisticsBolt extends AbstractDpsBolt {

  private static final long serialVersionUID = 1L;
  public static final Logger LOGGER = LoggerFactory.getLogger(StatisticsBolt.class);

  private final String hosts;
  private final int port;
  private final String keyspaceName;
  private final String userName;
  private final String password;
  private transient ValidationStatisticsServiceImpl statisticsService;
  private transient ProcessedRecordsDAO processedRecordsDAO;

  public StatisticsBolt(CassandraProperties cassandraProperties, String hosts, int port,
      String keyspaceName, String userName, String password) {
    super(cassandraProperties);
    this.hosts = hosts;
    this.port = port;
    this.keyspaceName = keyspaceName;
    this.userName = userName;
    this.password = password;
  }

  @Override
  public void prepare() {
    CassandraConnectionProvider cassandraConnectionProvider = CassandraConnectionProviderSingleton.getCassandraConnectionProvider(
        hosts, port, keyspaceName,
        userName, password);
    statisticsService = ValidationStatisticsServiceImpl.getInstance(cassandraConnectionProvider);
    processedRecordsDAO = ProcessedRecordsDAO.getInstance(cassandraConnectionProvider);
  }

  @Override
  public void execute(Tuple anchorTuple, StormTaskTuple stormTaskTuple) {
    try {
      if (statsShouldBeGenerated(stormTaskTuple)) {
        LOGGER.info("Calculating file statistics for {}", stormTaskTuple);
        countStatistics(stormTaskTuple);
        markRecordStatsAsCalculated(stormTaskTuple);
      } else {
        LOGGER.info("File stats will NOT be calculated for: {}", stormTaskTuple.getFileUrl());
      }
      // we can remove the file content before emitting further
      stormTaskTuple.setFileData((byte[]) null);
      outputCollector.emit(anchorTuple, stormTaskTuple.toStormTuple());
      outputCollector.ack(anchorTuple);
    } catch (RetryInterruptedException e) {
      handleInterruption(e, anchorTuple);
    } catch (Exception e) {
      emitErrorNotification(anchorTuple, stormTaskTuple, e.getMessage(), "Statistics for the given file could not be prepared.");
        outputCollector.ack(anchorTuple);
    }
  }

  private boolean statsShouldBeGenerated(StormTaskTuple stormTaskTuple) {
    return StormTaskTupleHelper.statisticsShouldBeGenerated(stormTaskTuple) && !statsAlreadyCalculated(stormTaskTuple);
  }

  private boolean statsAlreadyCalculated(StormTaskTuple stormTaskTuple) {
    Optional<ProcessedRecord> processingRecordStage = processedRecordsDAO.selectByPrimaryKey(stormTaskTuple.getTaskId(),
        stormTaskTuple.getFileUrl());
    return processingRecordStage.isPresent() &&
        EnumSet.of(RecordState.STATS_GENERATED, RecordState.ERROR, RecordState.SUCCESS)
               .contains(processingRecordStage.get().getState());
  }

  private void countStatistics(StormTaskTuple stormTaskTuple) throws ParserConfigurationException, SAXException, IOException {
    String document = new String(stormTaskTuple.getFileData(), StandardCharsets.UTF_8);
    RecordStatisticsGenerator statisticsGenerator = new RecordStatisticsGenerator(document);
    statisticsService.insertNodeStatistics(stormTaskTuple.getTaskId(), statisticsGenerator.getStatistics());
  }

  private void markRecordStatsAsCalculated(StormTaskTuple stormTaskTuple) {
    if (!statsAlreadyCalculated(stormTaskTuple)) {
      processedRecordsDAO.updateProcessedRecordState(stormTaskTuple.getTaskId(), stormTaskTuple.getFileUrl(),
          RecordState.STATS_GENERATED);
    }
  }
}
