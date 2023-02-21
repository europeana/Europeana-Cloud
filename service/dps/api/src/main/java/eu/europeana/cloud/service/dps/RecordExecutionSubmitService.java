package eu.europeana.cloud.service.dps;


/**
 * Service to fetch / submit tasks
 */
public interface RecordExecutionSubmitService {

  /**
   * Submits a record for execution.
   * <p>
   * Depending on the record-type and the record-owner, the {@link DpsRecord} will be submitted to a different Storm topology
   */
  void submitRecord(DpsRecord dpsRecord, String topic);

}
