package eu.europeana.cloud.service.mcs;

public class RestInterfaceConstants {

    private RestInterfaceConstants() {}

    //DataSetResource
    public static final String DATA_SET_RESOURCE =
            "/data-providers/{providerId}/data-sets/{dataSetId}";

    public static final String DATA_SET_PERMISSIONS_RESOURCE =
            "/data-providers/{providerId}/data-sets/{dataSetId}/permissions";

    public static final String DATA_SET_REPRESENTATIONS_NAMES =
            "/data-providers/{providerId}/data-sets/{dataSetId}/representationsNames";

    //DataSetRevisionsResource
    public static final String DATA_SET_REVISIONS_RESOURCE =
            "/data-providers/{providerId}/data-sets/{dataSetId}/representations/{representationName}/revisions/{revisionName}/revisionProvider/{revisionProviderId}";

    //DataSetsResource
    public static final String DATA_SETS_RESOURCE =
            "/data-providers/{providerId}/data-sets";

    //RecordsResource
    public static final String RECORDS_RESOURCE = "/records/{cloudId}";

    //RepresentationsResource
    public static final String REPRESENTATIONS_RESOURCE = "/records/{cloudId}/representations";

    //RepresentationResource
    public static final String REPRESENTATION_RESOURCE =
            "/records/{cloudId}/representations/{representationName}";


    //RepresentationVersionsResource
    public static final String REPRESENTATION_VERSIONS_RESOURCE =
            "/records/{cloudId}/representations/{representationName}/versions";

    //RepresentationVersionResource
    public static final String REPRESENTATION_VERSION =
            "/records/{cloudId}/representations/{representationName}/versions/{version:.+}";

    public static final String REPRESENTATION_VERSION_PERSIST =
            "/records/{cloudId}/representations/{representationName}/versions/{version}/persist";

    public static final String REPRESENTATION_VERSION_COPY =
            "/records/{cloudId}/representations/{representationName}/versions/{version}/copy";

    public static final String REPRESENTATION_PERMIT =
            "/records/{cloudId}/representations/{representationName}/versions/{version}/permit";

    //FilesResource
    public static final String FILES_RESOURCE =
            "/records/{cloudId}/representations/{representationName}/versions/{version}/files";

    //FileResource
    public static final String FILE_RESOURCE =
            "/records/{cloudId}/representations/{representationName}/versions/{version}/files/**";

    public static final String CLIENT_FILE_RESOURCE =
            "/records/{cloudId}/representations/{representationName}/versions/{version}/files/{fileName}";

    //FileUploadResource
    public static final String FILE_UPLOAD_RESOURCE =
            "/records/{cloudId}/representations/{representationName}/files";

    //RepresentationRevisionsResource
    public static final String REPRESENTATION_REVISIONS_RESOURCE =
            "/records/{cloudId}/representations/{representationName}/revisions/{revisionName}";

    //RevisionResource
    public static final String REVISION_ADD
            = "/records/{cloudId}/representations/{representationName}/versions/{version}/revisions";

    public static final String REVISION_ADD_WITH_PROVIDER_TAG
            = "/records/{cloudId}/representations/{representationName}/versions/{version}/revisions/{revisionName}/revisionProvider/{revisionProviderId}/tag/{tag}";

    public static final String REVISION_ADD_WITH_PROVIDER
            = "/records/{cloudId}/representations/{representationName}/versions/{version}/revisions/{revisionName}/revisionProvider/{revisionProviderId}/tags";

    public static final String REVISION_DELETE
            = "/records/{cloudId}/representations/{representationName}/versions/{version}/revisions/{revisionName}/revisionProvider/{revisionProviderId}";

    //SimplifiedFileAccessResource
    public static final String SIMPLIFIED_FILE_ACCESS_RESOURCE
            = "/data-providers/{providerId}/records/{localId:.+}/representations/{representationName}/{fileName:.+}";

    //SimplifiedRecordsResource
    public static final String SIMPLIFIED_RECORDS_RESOURCE
            = "/data-providers/{providerId}/records/{localId:.+}";

    //SimplifiedRepresentationResource
    public static final String SIMPLIFIED_REPRESENTATION_RESOURCE
            = "/data-providers/{providerId}/records/{localId:.+}/representations/{representationName}";
}
