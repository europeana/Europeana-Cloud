NOTE:
    this tool should be build by command 'mvn clean compile assembly:single'
    this tool now is settingup for ProviderCleanerTool class.
    For setting up it for TaskCleanerTool the *.pom file, configuration for maven-assembly-plugin mus be changed!!!

================================
DOC for TaskCleanerTool
================================

This tool could be used to clean notifications(logs), error reports and statistics of specific task/tasks.

The tool could be used in two different modes:
1- TO remove a specific task
        java -jar ecloud-cleaner-tool-1.0-SNAPSHOT-jar-with-dependencies.jar -hosts HOST -port 9042 -keyspace dps -password cassandra -username cassandra -taskId TASK_ID -remove_error_reports true
2- To remove a list of taskIds defined in a csv, or txt file.
        java -jar ecloud-cleaner-tool-1.0-SNAPSHOT-jar-with-dependencies.jar -hosts HOST -port 9042 -keyspace dps -password cassandra -username cassandra -task_Ids_file_path PATH -remove_error_reports true


================================
DOC for ProviderCleanerTool
================================

This tool could be used to clean records and/or datasets for given provider
Providers should be defined in simple text file with format

----- cut here begin ----------------
Provider-1_id
Provider-2_id
...
Provider-n_id
----- cut here end ----------------

comments (#) and empty lines are allowed:

----- cut here begin ----------------
Provider-1_id
Provider-2_id  #Skip this one ?? maybe

#Skip providers below
#Provider-3_id
#Provider-4_id
...
Provider-n_id
----- cut here end ----------------

0. Flag '--test-mode yes' is if we want to remove data "to the test"

1. Example parameteers to remove records data:
        java -jar ecloud-cleaner-tool-2.0-SNAPSHOT-jar-with-dependencies.jar --test-mode yes --only-records yes --url https://test.ecloud.psnc.pl/api --username admin --password ***** --providers-file /path/to/providers/file/file_described_above

        java -jar ecloud-cleaner-tool-2.0-SNAPSHOT-jar-with-dependencies.jar --test-mode yes --only-records yes --url https://ecloud.psnc.pl/api --username admin --password ***** --records-file /path/to/file/with/records_ids/file_described_above

2. Example parameteers to remove datasets:
        java -jar ecloud-cleaner-tool-2.0-SNAPSHOT-jar-with-dependencies.jar --only-datasets yes --url https://test.ecloud.psnc.pl/api --username metis_test --password ***** --providers-file /path/to/providers/file/file_described_above