package eu.europeana.cloud.mcs.driver;

/**
 * Created by Tarek on 4/23/2018.
 */
public class Test {
    public static void main(String[] args) throws Exception {
        DataSetServiceClient dataSetServiceClient = new DataSetServiceClient("http://195.216.97.81:8080/mcs", "mms_user", "mbUDx9vr");
        System.out.println(dataSetServiceClient.createDataSet("mms_prov", "DATASET_ID_BIG", "sdsd"));
    }
}
/*

    {
        "inputData":{
        "entry":[
        {
            "key":"REPOSITORY_URLS ",
                "value":"http://oai-pmh.eanadev.org/oai"
        }
      ]
    },
        "parameters":{
        "entry":[
        {
            "key":"PROVIDER_ID",
                "value":"mms_prov"
        },
        {
            "key":"OUTPUT_DATA_SETS",
                "value":"https://test-cloud.europeana.eu/api/data-providers/mms_prov/data-sets/DATASET_ID_BIG"
        }
      ]
    },
        "harvestingDetails":{
        "schemas":[
        "edm"
      ],
        "sets":[
        "2058621_Ag_EU_LoCloud_NRA"
      ]
    }
    }
}
*/
