package eu.europeana.cloud.service.dps.storm.topologies.administration;

import eu.europeana.cloud.common.properties.CassandraProperties;
import eu.europeana.cloud.service.commons.utils.RetryInterruptedException;
import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.tuple.common.CommonTaskTuple;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DummyBolt extends AbstractDpsBolt {

  private static final Logger LOGGER = LoggerFactory.getLogger(DummyBolt.class);

  public DummyBolt(CassandraProperties cassandraProperties) {
    super(cassandraProperties);
  }

  @Override
  public void execute(Tuple anchorTuple, CommonTaskTuple commonTaskTuple) {
    LOGGER.info("Dummy record processing: {} ...", commonTaskTuple.getRecordUri());
    try {
      commonTaskTuple.setFileData(commonTaskTuple.getFileData());

      outputCollector.emit(anchorTuple, commonTaskTuple.toStormTuple());
      outputCollector.ack(anchorTuple);
    } catch (RetryInterruptedException e) {
      handleInterruption(e, anchorTuple);
    } catch (Exception e) {
      LOGGER.error("DummyBolt error:{}", e.getMessage());
      emitErrorNotification(anchorTuple, commonTaskTuple, e.getMessage(), ExceptionUtils.getStackTrace(e));
      outputCollector.ack(anchorTuple);
    }
  }

  @Override
  public void prepare() {
  }
}
