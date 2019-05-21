This tool could be used to clean notifications(logs), error reports and statistics of specific task/tasks.

The tool could be used in two different modes:
1- TO remove a specific task
    java -jar ecloud-cleaner-tool-1.0-SNAPSHOT-jar-with-dependencies.jar -hosts HOST -port 9042 -keyspace dps -password cassandra -username cassandra -taskId TASK_ID -remove_error_reports true
2- To remove a list of taskIds defined in a csv, or txt file.
        java -jar ecloud-cleaner-tool-1.0-SNAPSHOT-jar-with-dependencies.jar -hosts HOST -port 9042 -keyspace dps -password cassandra -username cassandra -task_Ids_file_path PATH -remove_error_reports true