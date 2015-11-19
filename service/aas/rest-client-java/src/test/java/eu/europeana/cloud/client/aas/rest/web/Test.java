package eu.europeana.cloud.client.aas.rest.web;

import eu.europeana.cloud.client.aas.rest.AASClient;

/**
 * Created by Tarek on 11/18/2015.
 */
public class Test {

    private final static String AAS_BASE_URL = "http://iks-kbase.synat.pcss.pl:9090/aas";

    public static void main(String[] args) throws Exception {
        AASClient aasClient = new AASClient(AAS_BASE_URL, "admin", "admin");

        aasClient.createEcloudUser("tarokaaa", "tarokaaa");
    }
}

