package eu.europeana.cloud.service.dps.storm.io;


import static eu.europeana.cloud.service.dps.Constants.DPS_REPRESENTATION_NAME;
import static eu.europeana.cloud.service.dps.PluginParameterKeys.CLOUD_ID;

import eu.europeana.cloud.common.model.AnnotationsDto;
import eu.europeana.cloud.common.model.RepresentationVersionAnnotation;
import eu.europeana.cloud.common.properties.CassandraProperties;
import eu.europeana.cloud.mcs.driver.RecordServiceClient;
import eu.europeana.cloud.service.commons.urls.UrlParser;
import eu.europeana.cloud.service.commons.urls.UrlPart;
import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.tuple.common.CommonTaskTuple;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import java.net.MalformedURLException;
import java.util.UUID;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Stores a Record on the cloud.
 * <p/>
 * Receives a byte array representing a Record from a tuple, creates and stores a new Record on the cloud, and emits the URL of
 * the newly created record.
 */
public abstract class AbstractAnnotationBolt extends AbstractDpsBolt {

  private static final long serialVersionUID = 1L;
  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractAnnotationBolt.class);
  private final String ecloudMcsAddress;
  private final String topologyUserName;
  private final String topologyUserPassword;
  protected transient RecordServiceClient recordServiceClient;

  public AbstractAnnotationBolt(CassandraProperties cassandraProperties, String ecloudMcsAddress, String topologyUserName,
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
  }

  @Override
  public void execute(Tuple anchorTuple, CommonTaskTuple commonTaskTuple) throws MalformedURLException, MCSException {
    LOGGER.debug("AbstractAnnotationBolt: adding annotation to record: {}", commonTaskTuple.getRecordUri());
    String fileUrl = commonTaskTuple.getRecordUri();
    UrlParser urlParser = new UrlParser(fileUrl);
    String cloudId = urlParser.getPart(UrlPart.RECORDS);
    String version = urlParser.getPart(UrlPart.VERSIONS);
    recordServiceClient.addAnnotationToRepresentationVersion(
        cloudId, DPS_REPRESENTATION_NAME, UUID.fromString(version), new AnnotationsDto(createAnnotation(commonTaskTuple)));
    emitSuccessfulResult(anchorTuple, commonTaskTuple);
  }

  protected abstract RepresentationVersionAnnotation createAnnotation(CommonTaskTuple tuple);

  protected void emitSuccessfulResult(Tuple anchorTuple, CommonTaskTuple commonTaskTuple) {
    emitNotification(anchorTuple, commonTaskTuple);
  }

}

