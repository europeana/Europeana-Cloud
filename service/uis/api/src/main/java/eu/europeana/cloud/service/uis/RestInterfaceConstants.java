package eu.europeana.cloud.service.uis;

public final class RestInterfaceConstants {

  //DataSetAssignmentsResource
  public static final String DATA_PROVIDERS = "/data-providers";
  public static final String DATA_PROVIDER = "/data-providers/{providerId}";
  public static final String CLOUD_IDS = "/cloudIds";
  public static final String CLOUD_ID = "/cloudIds/{cloudId}";
  public static final String DATA_PROVIDER_ACTIVATION = "/data-providers/{providerId}/active";
  public static final String CLOUD_ID_TO_RECORD_ID_MAPPING = "/data-providers/{providerId}/cloudIds/{cloudId}";
  public static final String RECORD_ID_MAPPING_REMOVAL = "/data-providers/{providerId}/localIds/{recordId}";

  private RestInterfaceConstants() {
  }

}
