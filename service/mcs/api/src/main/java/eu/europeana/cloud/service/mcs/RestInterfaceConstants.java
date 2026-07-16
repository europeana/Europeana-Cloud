package eu.europeana.cloud.service.mcs;

/**
 * Class containing constants used by mcs rest interface
 */
public final class RestInterfaceConstants {

  //DataSetResource
  public static final String DATA_SET_RESOURCE =
      "/data-providers/{providerId}/data-sets/{dataSetId}";

  public static final String DATA_SET_PERMISSIONS_RESOURCE =
      DATA_SET_RESOURCE + "/permissions";

  public static final String DATA_SET_REPRESENTATIONS_NAMES =
      DATA_SET_RESOURCE + "/representationsNames";

  //DataSetsResource
  public static final String DATA_SETS_RESOURCE =
      "/data-providers/{providerId}/data-sets";

  //RecordsResource
  public static final String RECORDS_RESOURCE = "/records/{cloudId}";

  //RepresentationsResource
  public static final String REPRESENTATIONS_RESOURCE = RECORDS_RESOURCE + "/representations";

  //RepresentationResource
  public static final String REPRESENTATION_RESOURCE =
      REPRESENTATIONS_RESOURCE + "/{representationName}";


  //RepresentationVersionsResource
  public static final String REPRESENTATION_VERSIONS_RESOURCE =
      REPRESENTATION_RESOURCE + "/versions";

  //RepresentationVersionResource
  public static final String REPRESENTATION_VERSION =
      REPRESENTATION_VERSIONS_RESOURCE + "/{version:.+}";

  public static final String REPRESENTATION_VERSION_PERSIST =
      REPRESENTATION_VERSIONS_RESOURCE + "/{version}/persist";

  public static final String REPRESENTATION_VERSION_COPY =
      REPRESENTATION_VERSIONS_RESOURCE + "/{version}/copy";

  public static final String REPRESENTATION_PERMIT =
      REPRESENTATION_VERSIONS_RESOURCE + "/{version}/permit";

  //FilesResource
  public static final String FILES_RESOURCE =
      REPRESENTATION_VERSIONS_RESOURCE + "/{version}/files";

  //FileResource
  public static final String FILE_RESOURCE =
      REPRESENTATION_VERSIONS_RESOURCE + "/{version}/files/**";

  public static final String CLIENT_FILE_RESOURCE =
      REPRESENTATION_VERSIONS_RESOURCE + "/{version}/files/{fileName}";

  //FileUploadResource
  public static final String FILE_UPLOAD_RESOURCE =
      "/records/{cloudId}/representations/{representationName}/files";

   //SimplifiedRecordsResource
  public static final String SIMPLIFIED_RECORDS_RESOURCE =
      "/data-providers/{providerId}/records/{localId:.+}";

  //Annotations
  public static final String REPRESENTATION_VERSION_ANNOTATION_RESOURCE
      = "/records/{cloudId}/representations/{representationName}/versions/{version}/annotations";

  //SimplifiedFileAccessResource
  public static final String SIMPLIFIED_FILE_ACCESS_RESOURCE =
      SIMPLIFIED_RECORDS_RESOURCE + "/representations/{representationName}/**";

  //SimplifiedRepresentationResource
  public static final String SIMPLIFIED_REPRESENTATION_RESOURCE =
      SIMPLIFIED_RECORDS_RESOURCE + "/representations/{representationName}";

  private RestInterfaceConstants() {
  }
}
