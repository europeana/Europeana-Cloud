Data Validator
1- This tool could be used in one of those two scenarios:
    a- Specify the source and target table names; and check the data integrity between them:
        java -jar data-validator-0.7-SNAPSHOT.jar source -sourceTable table -targetTable table
    b- Not to specify the source and target tables names; In this case check the data integrity between all tha tables inside the source and target keyspaces:
        java -jar data-validator-0.7-SNAPSHOT.jar

2- In those both scenarios a properties file is used to configure cassandra connection. The default one is test.properties located inside resource folder
and if you want to override it you can simply create your own properties file with the same properties keys and pass it to the tool by specifying the configuration argument:
    java -jar data-validator-0.7-SNAPSHOT.jar -configuration newTest.properties

3- Another optional argument could be passed to the tool which is -threads : define how many threads per table should handle the validation process ;Default value =100;

