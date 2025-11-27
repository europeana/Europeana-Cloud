package eu.europeana.cloud.enrichment.bolts;

import eu.europeana.cloud.common.properties.CassandraProperties;
import eu.europeana.cloud.service.commons.utils.RetryInterruptedException;
import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.BoltInitializationException;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.enrichment.rest.client.EnrichmentWorker;
import eu.europeana.enrichment.rest.client.EnrichmentWorkerImpl;
import eu.europeana.enrichment.rest.client.dereference.Dereferencer;
import eu.europeana.enrichment.rest.client.dereference.DereferencerProvider;
import eu.europeana.enrichment.rest.client.enrichment.Enricher;
import eu.europeana.enrichment.rest.client.enrichment.EnricherProvider;
import eu.europeana.enrichment.rest.client.report.ProcessedResult;
import eu.europeana.enrichment.rest.client.report.ProcessedResult.RecordStatus;
import eu.europeana.enrichment.rest.client.report.Report;
import eu.europeana.enrichment.rest.client.report.Type;
import java.nio.charset.StandardCharsets;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Call the remote enrichment service in order to dereference and enrich a file.
 * <p/>
 * Receives a byte array representing a Record from a tuple, enrich its content nad store it as part of the emitted tuple.
 */
public class EnrichmentBolt extends AbstractDpsBolt {

  private static final long serialVersionUID = 1L;
  private static final Logger LOGGER = LoggerFactory.getLogger(EnrichmentBolt.class);

  private final String dereferenceURL;
  private final String enrichmentEntityManagementUrl;
  private final String enrichmentEntityApiUrl;
  private final String enrichmentEntityApiTokenEndpoint;
  private final String enrichmentEntityApiGrantParams;
  private transient EnrichmentWorker enrichmentWorker;

  public EnrichmentBolt(CassandraProperties cassandraProperties, String dereferenceURL, String enrichmentEntityManagementUrl,
      String enrichmentEntityApiUrl, String enrichmentEntityApiTokenEndpoint, String enrichmentEntityApiGrantParams) {
    super(cassandraProperties);
    this.dereferenceURL = dereferenceURL;
    this.enrichmentEntityManagementUrl = enrichmentEntityManagementUrl;
    this.enrichmentEntityApiUrl = enrichmentEntityApiUrl;
    this.enrichmentEntityApiTokenEndpoint = enrichmentEntityApiTokenEndpoint;
    this.enrichmentEntityApiGrantParams = enrichmentEntityApiGrantParams;
  }

  @Override
  public void execute(Tuple anchorTuple, StormTaskTuple stormTaskTuple) {
    try {
      LOGGER.info("starting enrichment on {} .....", stormTaskTuple.getFileUrl());
      String fileContent = new String(stormTaskTuple.getFileData(), StandardCharsets.UTF_8);
      ProcessedResult<String> result = enrichmentWorker.process(fileContent);
      Set<Report> reports = filterOutIgnoredReports(result.getReport());
      if (shouldRecordBeFurtherProcessed(result)) {
        processRecord(anchorTuple, stormTaskTuple, result, reports);
      } else {
        dropRecord(anchorTuple, stormTaskTuple, reports);
      }
      outputCollector.ack(anchorTuple);
      LOGGER.info("Completed enrichment.");
    } catch (RetryInterruptedException e) {
      handleInterruption(e, anchorTuple);
    } catch (Exception e) {
      handleProcessingError(anchorTuple, stormTaskTuple, e);
    }
  }

  private Set<Report> filterOutIgnoredReports(Set<Report> reports) {
    return reports
            .stream()
            .filter(rm -> rm.getMessageType() != Type.IGNORE)
            .collect(Collectors.toSet());
  }

  private void handleProcessingError(Tuple anchorTuple, StormTaskTuple stormTaskTuple, Exception e) {
    LOGGER.error("Exception while Enriching/dereference", e);
    emitErrorNotification(anchorTuple,
            stormTaskTuple,
        e.getMessage(),
        "Remote Enrichment/dereference service caused the problem!. The full error: "
            + ExceptionUtils.getStackTrace(e));
    outputCollector.ack(anchorTuple);
  }

  private void dropRecord(Tuple anchorTuple, StormTaskTuple stormTaskTuple, Set<Report> reports) {
    LOGGER.error("Error occurred during process of enrichment");
    stormTaskTuple.addReports(reports);
    Integer errorReportCount = reports
            .stream().filter(
                    rm -> rm.getMessageType() == Type.ERROR
            ).collect(Collectors.toSet()).size();
    String errorAdditionalInformation = String.format("Number of errors that occurred during enrichment: %d", errorReportCount);
    emitErrorNotification(anchorTuple,
            stormTaskTuple,
            "Error occurred during enrichment/dereference process",
            errorAdditionalInformation);
  }

  private void processRecord(Tuple anchorTuple, StormTaskTuple stormTaskTuple, ProcessedResult<String> result, Set<Report> filteredReports) throws Exception {
    LOGGER.info("Finishing enrichment on {} .....", stormTaskTuple.getFileUrl());
    emitEnrichedContent(anchorTuple, stormTaskTuple, result, filteredReports);
    LOGGER.info("Emitted enriched record on {}", stormTaskTuple.getFileUrl());
  }

  private boolean shouldRecordBeFurtherProcessed(ProcessedResult<String> result) {
    return RecordStatus.CONTINUE.equals(result.getRecordStatus());
  }

  @Override
  public void prepare() {
    final EnricherProvider enricherProvider = new EnricherProvider();
    enricherProvider.setEnrichmentPropertiesValues(enrichmentEntityManagementUrl, enrichmentEntityApiUrl,
        enrichmentEntityApiTokenEndpoint, enrichmentEntityApiGrantParams);
    final DereferencerProvider dereferencerProvider = new DereferencerProvider();
    dereferencerProvider.setDereferenceUrl(dereferenceURL);
    dereferencerProvider.setEnrichmentPropertiesValues(enrichmentEntityManagementUrl, enrichmentEntityApiUrl,
        enrichmentEntityApiTokenEndpoint, enrichmentEntityApiGrantParams);
    try (Dereferencer dereferencer = dereferencerProvider.create(); Enricher enricher = enricherProvider.create()) {
      enrichmentWorker = new EnrichmentWorkerImpl(dereferencer, enricher);
    } catch (Exception e) {
      for (Throwable s : e.getSuppressed()) {
        LOGGER.warn("EnrichmentBolt suppressed exception during close", s);
      }
      throw new BoltInitializationException("Could not instantiate EnrichmentBolt due Exception in enrich worker creating", e);
    }
  }

  private void emitEnrichedContent(Tuple anchorTuple, StormTaskTuple stormTaskTuple, ProcessedResult<String> result, Set<Report> reports)
          throws Exception {
    prepareStormTaskTupleForEmission(stormTaskTuple, result.getProcessedRecord());
    stormTaskTuple.addReports(reports);
    outputCollector.emit(anchorTuple, stormTaskTuple.toStormTuple());
  }
}
