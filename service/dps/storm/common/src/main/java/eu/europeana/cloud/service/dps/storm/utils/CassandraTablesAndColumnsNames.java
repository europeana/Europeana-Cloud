package eu.europeana.cloud.service.dps.storm.utils;

public final class CassandraTablesAndColumnsNames {

  //------- TABLES -------
  public static final String TASK_INFO_TABLE = "task_info";
  public static final String NOTIFICATIONS_TABLE = "notifications";
  public static final String ERROR_NOTIFICATIONS_TABLE = "error_notifications";
  public static final String ERROR_TYPES_TABLE = "error_types";
  public static final String GENERAL_STATISTICS_TABLE = "general_statistics";
  public static final String NODE_STATISTICS_TABLE = "node_statistics";
  public static final String ATTRIBUTE_STATISTICS_TABLE = "attribute_statistics";
  public static final String STATISTICS_REPORTS_TABLE = "statistics_reports";
  public static final String TASKS_BY_STATE_TABLE = "tasks_by_task_state";
  public static final String PROCESSED_RECORDS_TABLE = "processed_records";
  public static final String HARVESTED_RECORD_TABLE = "harvested_records";

  //------- TASK INFO -------
  public static final String TASK_INFO_TASK_ID = "task_id";
  public static final String TASK_INFO_TOPOLOGY_NAME = "topology_name";
  public static final String TASK_INFO_STATE = "state";
  public static final String TASK_INFO_STATE_DESCRIPTION = "state_description";
  public static final String TASK_INFO_SENT_TIMESTAMP = "sent_timestamp";
  public static final String TASK_INFO_START_TIMESTAMP = "start_timestamp";
  public static final String TASK_INFO_FINISH_TIMESTAMP = "finish_timestamp";
  public static final String TASK_INFO_EXPECTED_RECORDS_NUMBER = "expected_records_number";
  public static final String TASK_INFO_PROCESSED_RECORDS_COUNT = "processed_records_count";
  public static final String TASK_INFO_IGNORED_RECORDS_COUNT = "ignored_records_count";
  public static final String TASK_INFO_DELETED_RECORDS_COUNT = "deleted_records_count";
  public static final String TASK_INFO_PROCESSED_ERRORS_COUNT = "processed_errors_count";
  public static final String TASK_INFO_DELETED_ERRORS_COUNT = "deleted_errors_count";
  public static final String TASK_INFO_EXPECTED_POST_PROCESSED_RECORDS_NUMBER = "expected_post_processed_records_number";
  public static final String TASK_INFO_POST_PROCESSED_RECORDS_COUNT = "post_processed_records_count";
  public static final String TASK_INFO_DEFINITION = "definition";


  //-------- TASK_DIAGNOSTIC_INFO --------
  public static final String TASK_DIAGNOSTIC_INFO_TABLE = "task_diagnostic_info";
  public static final String TASK_DIAGNOSTIC_INFO_ID = "task_id";
  public static final String TASK_DIAGNOSTIC_INFO_STARTED_RECORDS_COUNT = "started_records_count";
  public static final String TASK_DIAGNOSTIC_INFO_RECORDS_RETRY_COUNT = "records_retry_count";
  public static final String TASK_DIAGNOSTIC_INFO_QUEUED_TIME = "queued_time";
  public static final String TASK_DIAGNOSTIC_INFO_START_ON_STORM_TIME = "start_on_storm_time";
  public static final String TASK_DIAGNOSTIC_INFO_FINISH_ON_STORM_TIME = "finish_on_storm_time";
  public static final String TASK_DIAGNOSTIC_INFO_LAST_RECORD_FINISHED_ON_STORM_TIME = "last_record_finished_on_storm_time";
  public static final String TASK_DIAGNOSTIC_INFO_POST_PROCESSING_START_TIME = "post_processing_start_time";


  //------- NOTIFICATION -------
  public static final String NOTIFICATION_TASK_ID = "task_id";
  public static final String NOTIFICATION_BUCKET_NUMBER = "bucket_number";
  public static final String NOTIFICATION_RESOURCE_NUM = "resource_num";
  public static final String NOTIFICATION_TOPOLOGY_NAME = "topology_name";
  public static final String NOTIFICATION_RESOURCE = "resource";
  public static final String NOTIFICATION_STATE = "state";
  public static final String NOTIFICATION_INFO_TEXT = "info_text";
  public static final String NOTIFICATION_ADDITIONAL_INFORMATION = "additional_information";
  public static final String NOTIFICATION_RESULT_RESOURCE = "result_resource";


  //-------- ERROR NOTIFICATION ---------
  public static final String ERROR_NOTIFICATION_TASK_ID = "task_id";
  public static final String ERROR_NOTIFICATION_ERROR_TYPE = "error_type";
  public static final String ERROR_NOTIFICATION_ERROR_MESSAGE = "error_message";
  public static final String ERROR_NOTIFICATION_RESOURCE = "resource";
  public static final String ERROR_NOTIFICATION_ADDITIONAL_INFORMATIONS = "additional_informations";


  //-------- ERROR COUNTERS ----------
  public static final String ERROR_TYPES_TASK_ID = "task_id";
  public static final String ERROR_TYPES_ERROR_TYPE = "error_type";
  public static final String ERROR_TYPES_COUNTER = "error_count";


  //-------- GENERAL STATISTICS ----------
  public static final String GENERAL_STATISTICS_TASK_ID = "task_id";
  public static final String GENERAL_STATISTICS_PARENT_XPATH = "parent_xpath";
  public static final String GENERAL_STATISTICS_NODE_XPATH = "node_xpath";
  public static final String GENERAL_STATISTICS_OCCURRENCE = "occurrence";


  //-------- NODE STATISTICS ----------
  public static final String NODE_STATISTICS_TASK_ID = "task_id";
  public static final String NODE_STATISTICS_NODE_XPATH = "node_xpath";
  public static final String NODE_STATISTICS_VALUE = "node_value";
  public static final String NODE_STATISTICS_OCCURRENCE = "occurrence";


  //-------- ATTRIBUTE STATISTICS ----------
  public static final String ATTRIBUTE_STATISTICS_TASK_ID = "task_id";
  public static final String ATTRIBUTE_STATISTICS_NODE_XPATH = "node_xpath";
  public static final String ATTRIBUTE_STATISTICS_NODE_VALUE = "node_value";
  public static final String ATTRIBUTE_STATISTICS_NAME = "attribute_name";
  public static final String ATTRIBUTE_STATISTICS_VALUE = "attribute_value";
  public static final String ATTRIBUTE_STATISTICS_OCCURRENCE = "occurrence";

  //--------- STATISTICS REPORTS ------------
  public static final String STATISTICS_REPORTS_TASK_ID = "task_id";
  public static final String STATISTICS_REPORTS_REPORT_DATA = "report_data";

  //-------TASKS_BY_STATE_TABLE----------------
  public static final String TASKS_BY_STATE_STATE_COL_NAME = "state";
  public static final String TASKS_BY_STATE_TOPOLOGY_NAME = "topology_name";
  public static final String TASKS_BY_STATE_TASK_ID_COL_NAME = "task_id";
  public static final String TASKS_BY_STATE_APP_ID_COL_NAME = "application_id";
  public static final String TASKS_BY_STATE_TOPIC_NAME_COL_NAME = "topic_name";
  public static final String TASKS_BY_STATE_START_TIME = "start_time";

  //------- PROCESSED_RECORDS -------
  public static final String PROCESSED_RECORDS_TASK_ID = "task_id";
  public static final String PROCESSED_RECORDS_RECORD_ID = "record_id";
  public static final String PROCESSED_RECORDS_ATTEMPT_NUMBER = "attempt_number";
  public static final String PROCESSED_RECORDS_DST_IDENTIFIER = "dst_identifier";
  public static final String PROCESSED_RECORDS_TOPOLOGY_NAME = "topology_name";
  public static final String PROCESSED_RECORDS_STATE = "state";
  public static final String PROCESSED_RECORDS_START_TIME = "start_time";
  public static final String PROCESSED_RECORDS_INFO_TEXT = "info_text";
  public static final String PROCESSED_RECORDS_ADDITIONAL_INFORMATIONS = "additional_informations";
  public static final String PROCESSED_RECORDS_BUCKET_NUMBER = "bucket_number";
  //------- HARVESTED_RECORD -------
  public static final String HARVESTED_RECORD_METIS_DATASET_ID = "metis_dataset_id";
  public static final String HARVESTED_RECORD_BUCKET_NUMBER = "bucket_number";
  public static final String HARVESTED_RECORD_LOCAL_ID = "record_local_id";
  public static final String HARVESTED_RECORD_LATEST_HARVEST_DATE = "latest_harvest_date";
  public static final String HARVESTED_RECORD_LATEST_HARVEST_MD5 = "latest_harvest_md5";
  public static final String HARVESTED_RECORD_PUBLISHED_HARVEST_DATE = "published_harvest_date";
  public static final String HARVESTED_RECORD_PUBLISHED_HARVEST_MD5 = "published_harvest_md5";
  public static final String HARVESTED_RECORD_PREVIEW_HARVEST_DATE = "preview_harvest_date";
  public static final String HARVESTED_RECORD_PREVIEW_HARVEST_MD5 = "preview_harvest_md5";

  private CassandraTablesAndColumnsNames() {
  }
}
