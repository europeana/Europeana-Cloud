package eu.europeana.cloud.service.dps.storm.io;


import static eu.europeana.cloud.service.dps.Constants.DPS_REPRESENTATION_NAME;

import eu.europeana.cloud.common.properties.CassandraProperties;
import eu.europeana.cloud.mcs.driver.DataSetServiceClient;
import eu.europeana.cloud.mcs.driver.RecordServiceClient;
import eu.europeana.cloud.service.commons.urls.UrlParser;
import eu.europeana.cloud.service.commons.urls.UrlPart;
import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.tuple.common.CommonTaskTuple;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import java.net.MalformedURLException;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Adds representation version of record to the dataset.
 */
public class AddToDatasetBolt extends AbstractDpsBolt {

  private static final long serialVersionUID = 1L;
  private static final Logger LOGGER = LoggerFactory.getLogger(AddToDatasetBolt.class);
  private final String ecloudMcsAddress;
  private final String topologyUserName;
  private final String topologyUserPassword;
  protected transient RecordServiceClient recordServiceClient;
  protected transient DataSetServiceClient dataSetServiceClient;

  public AddToDatasetBolt(CassandraProperties cassandraProperties, String ecloudMcsAddress, String topologyUserName,
      String topologyUserPassword) {
    super(cassandraProperties);
    this.ecloudMcsAddress = ecloudMcsAddress;
    this.topologyUserName = topologyUserName;
    this.topologyUserPassword = topologyUserPassword;
  }

  @Override
  protected boolean ignoreDeleted() {
    return false;
  }

  @Override
  public void prepare() {
    LOGGER.debug("Preparing MCS client with the following params url={} user={}", ecloudMcsAddress, topologyUserName);
    recordServiceClient = new RecordServiceClient(ecloudMcsAddress, topologyUserName, topologyUserPassword);
    dataSetServiceClient = new DataSetServiceClient(ecloudMcsAddress, topologyUserName, topologyUserPassword);
  }

  @Override
  public void execute(Tuple anchorTuple, CommonTaskTuple commonTaskTuple) throws MalformedURLException, MCSException {
    LOGGER.debug("AddToDatasetBolt: adding representation version to dataset: {}", commonTaskTuple.getOutputDatasetId());
    String fileUrl = commonTaskTuple.getRecordUri();
    UrlParser urlParser = new UrlParser(fileUrl);
    String cloudId = urlParser.getPart(UrlPart.RECORDS);
    String version = urlParser.getPart(UrlPart.VERSIONS);
    dataSetServiceClient.assignRepresentationVersionToDataset(commonTaskTuple.getOutputDatasetProvider(),commonTaskTuple.getOutputDatasetId(),
        cloudId,DPS_REPRESENTATION_NAME,version);
    emitSuccessfulResult(anchorTuple, commonTaskTuple);
  }

  protected void emitSuccessfulResult(Tuple anchorTuple, CommonTaskTuple commonTaskTuple) {
    emitNotification(anchorTuple, commonTaskTuple);
  }

}

