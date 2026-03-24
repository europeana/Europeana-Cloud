package eu.europeana.cloud.service.dps.storm.io;

import com.google.gson.Gson;
import eu.europeana.cloud.common.properties.CassandraProperties;
import eu.europeana.cloud.common.utils.Clock;
import eu.europeana.cloud.service.commons.utils.RetryInterruptedException;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.BoltInitializationException;
import eu.europeana.cloud.service.dps.storm.tuple.common.CommonTaskTuple;
import eu.europeana.cloud.service.dps.storm.utils.FileDataChecker;
import eu.europeana.metis.mediaprocessing.RdfConverterFactory;
import eu.europeana.metis.mediaprocessing.RdfDeserializer;
import eu.europeana.metis.mediaprocessing.exception.RdfDeserializationException;
import eu.europeana.metis.mediaprocessing.model.RdfResourceEntry;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.time.Instant;
import java.util.List;

/**
 * Created by Tarek on 12/6/2018.
 */
public abstract class ParseFileBolt extends ReadFileBolt {

  private static final Logger LOGGER = LoggerFactory.getLogger(ParseFileBolt.class);
  private transient Gson gson;
  protected transient RdfDeserializer rdfDeserializer;

  public ParseFileBolt(CassandraProperties cassandraProperties, String ecloudMcsAddress,
      String ecloudMcsUser, String ecloudMcsUserPassword) {
    super(cassandraProperties, ecloudMcsAddress, ecloudMcsUser, ecloudMcsUserPassword);
  }

  protected abstract List<RdfResourceEntry> getResourcesFromRDF(byte[] bytes) throws RdfDeserializationException;

  protected CommonTaskTuple createStormTuple(CommonTaskTuple commonTaskTuple, RdfResourceEntry rdfResourceEntry, int linksCount) {
    CommonTaskTuple tuple = SerializationUtils.clone(commonTaskTuple);
    LOGGER.debug("Sending this resource link {} to be processed ", rdfResourceEntry.getResourceUrl());
    tuple.addParameter(PluginParameterKeys.RESOURCE_LINK_KEY, gson.toJson(rdfResourceEntry));
    tuple.addParameter(PluginParameterKeys.RESOURCE_LINKS_COUNT, String.valueOf(linksCount));
    tuple.addParameter(PluginParameterKeys.RESOURCE_URL, rdfResourceEntry.getResourceUrl());
    return tuple;
  }

  protected abstract int getLinksCount(CommonTaskTuple tuple, int resourcesCount) throws RdfDeserializationException;

  @Override
  public void execute(Tuple anchorTuple, CommonTaskTuple commonTaskTuple) {
    LOGGER.debug("Starting file parsing");
    Instant processingStartTime = Instant.now();
    try (InputStream stream = getFileStreamByStormTuple(commonTaskTuple)) {
      byte[] fileContent = IOUtils.toByteArray(stream);
      if (FileDataChecker.isFileDataNullOrBlank(fileContent)) {
        LOGGER.warn("File data to be parsed is null or blank!");
      }
      List<RdfResourceEntry> rdfResourceEntries = getResourcesFromRDF(fileContent);
      int linksCount = getLinksCount(commonTaskTuple, rdfResourceEntries.size());
      if (linksCount == 0) {
        CommonTaskTuple tuple = SerializationUtils.clone(commonTaskTuple);
        LOGGER.warn("The EDM file has no resource Links ");
        outputCollector.emit(anchorTuple, tuple.toStormTuple());
      } else {
        LOGGER.debug("Found {} resources for {} : {}", rdfResourceEntries.size(),
                commonTaskTuple.getRecordUri(),
                rdfResourceEntries);
        for (RdfResourceEntry rdfResourceEntry : rdfResourceEntries) {
          if (taskStatusChecker.hasDroppedStatus(commonTaskTuple.getTaskId())) {
            break;
          }
          CommonTaskTuple tuple = createStormTuple(commonTaskTuple, rdfResourceEntry,
              Integer.parseInt(commonTaskTuple.getParameter(PluginParameterKeys.RESOURCE_LINKS_COUNT)));
          outputCollector.emit(anchorTuple, tuple.toStormTuple());
        }
      }
      outputCollector.ack(anchorTuple);
    } catch (RetryInterruptedException e) {
      handleInterruption(e, anchorTuple);
    } catch (Exception e) {
        LOGGER.error("Unable to read and parse file ", e);
        emitErrorNotification(anchorTuple, commonTaskTuple, e.getMessage(),
                "Error while reading and parsing the EDM file. The full error is: " + ExceptionUtils.getStackTrace(e));
        outputCollector.ack(anchorTuple);
    }
    LOGGER.info("File parsing finished in: {}ms", Clock.millisecondsSince(processingStartTime));
  }

  @Override
  public void prepare() {
    super.prepare();
    try {
      rdfDeserializer = new RdfConverterFactory().createRdfDeserializer();
      gson = new Gson();
    } catch (Exception e) {
      throw new BoltInitializationException("Unable to initialize RDF Deserializer", e);
    }
  }
}
