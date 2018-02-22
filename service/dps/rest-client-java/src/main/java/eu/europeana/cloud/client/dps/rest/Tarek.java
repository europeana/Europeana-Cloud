package eu.europeana.cloud.client.dps.rest;

/**
 * Created by Tarek on 2/20/2018.
 */
public class Tarek {
    public static void main(String[] args) throws Exception {
        DpsClient dpsClient = new DpsClient("http://localhost:8080/services", "admin", "admin");
        System.out.println(dpsClient.killTask("ic_topology", 6446998217671721753l));

    }
}
