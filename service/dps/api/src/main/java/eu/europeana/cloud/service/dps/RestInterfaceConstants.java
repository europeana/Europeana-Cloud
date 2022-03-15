package eu.europeana.cloud.service.dps;

public class RestInterfaceConstants {
    public static final String TOPOLOGY_NAME = "topologyName";
    public static final String TASK_ID = "taskId";
    public static final String DATASET_ID = "datasetId";
    public static final String ERROR = "error";
    public static final String IDS_COUNT = "idsCount";

    public static final String PERMIT_TOPOLOGY_URL      = "/{topologyName}/permit";
    public static final String TASKS_URL                = "/{topologyName}/tasks";
    public static final String TASK_PROGRESS_URL        = "/{topologyName}/tasks/{taskId}/progress";
    public static final String TASK_CLEAN_DATASET_URL   = "/{topologyName}/tasks/{taskId}/cleaner";
    public static final String KILL_TASK_URL            = "/{topologyName}/tasks/{taskId}/kill";
    public static final String STATISTICS_REPORT_URL    = "/{topologyName}/tasks/{taskId}/statistics";
    public static final String DETAILED_TASK_REPORT_URL = "/{topologyName}/tasks/{taskId}/reports/details";
    public static final String ERRORS_TASK_REPORT_URL   = "/{topologyName}/tasks/{taskId}/reports/errors";
    public static final String ELEMENT_REPORT_URL       = "/{topologyName}/tasks/{taskId}/reports/element";

    //DataSetAssignmentsResource
    public static final String METIS_DATASETS                         = "/metis-datasets/{datasetId}";
    public static final String METIS_DATASET_PUBLISHED_RECORDS_SEARCH = "/metis-datasets/{datasetId}/records/published/search";

    private RestInterfaceConstants() {}
}
