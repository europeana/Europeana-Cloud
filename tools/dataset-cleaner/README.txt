DataSet cleaner Tool
1- The tool is designed to remove all versions assigned to a dataSet. The tool will
remove all the versions files, revisions and unassign them from all the dataSets and
NOT JUST THE PASSED DATASET Because in the end it will REMOVE the representation version ENTIRELY
from the System.

2- To execute the tool (pass the correct parameters to the jar file). Example:
java -jar dataset-cleaner-1.1-SNAPSHOT-jar-with-dependencies --DATASET_URL http://localhost:8080/mcs/data-providers/provider/data-sets/dataset --MCS_URL http://localhost:8080/mcs --USERNAME username --PASSWORD password

