package eu.europeana.cloud.service.dps;

/**
 * Interface for the DpsTask input and output which is stored in MCS service
 */
public interface MCSInputOutput {
  String getProviderId();
  String getDatasetId();
  String getRepresentationName();
}
