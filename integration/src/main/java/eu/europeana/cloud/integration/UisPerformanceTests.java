/* UisPerformanceTests.java - created on Mar 24, 2014, Copyright (c) 2011 The European Library, all rights reserved */
package eu.europeana.cloud.integration;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.jettison.JettisonFeature;
import org.glassfish.jersey.jsonp.JsonProcessingFeature;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.glassfish.jersey.moxy.json.MoxyJsonFeature;

import eu.europeana.cloud.common.model.CloudId;
import eu.europeana.cloud.common.model.DataProviderProperties;
import eu.europeana.cloud.common.web.UISParamConstants;

/**
 * 
 * 
 * @author Markus Muhr (markus.muhr@kb.nl)
 * @since Mar 24, 2014
 */
public class UisPerformanceTests {
    /**
     * @param args
     */
    public static void main(String[] args) {
        Client client = ClientBuilder.newBuilder().register(JettisonFeature.class).register(
                JacksonFeature.class).register(MoxyJsonFeature.class).register(
                MultiPartFeature.class).register(JsonProcessingFeature.class).build();
        String baseUrl = "http://store2.tel.ulcc.ac.uk/";//"http://localhost:8080/";
        String UIS_PREFIX = "ecloud-service-uis-rest";

        long startTime = System.nanoTime();

        DataProviderProperties dp = new DataProviderProperties("TEL", "", "", "", "", "",
                "Ingesion Tool Provider", "");
        Response resp = client.target(baseUrl + UIS_PREFIX + "/data-providers").queryParam(
                UISParamConstants.Q_PROVIDER, "TEL").request().post(Entity.json(dp));
        if (resp.getStatus() == Status.OK.getStatusCode()) {
            System.out.println("Provider '" + "TEL" + "' has been created!");
        } else {
            System.out.println("Provider '" + "TEL" + "' exists!");
        }

        System.out.println("Provider setup: '" + ((System.nanoTime() - startTime) / 1000000) +
                           "' msec");

        long getCloudIdTime = 0;
        long createCloudIdTime = 0;

        for (int i = 0; i < 100000; i++) {
            long recordId = System.nanoTime();
            
            long localTime = System.nanoTime();
            CloudId cloudId;
            resp = client.target(baseUrl + UIS_PREFIX + "/cloudIds").queryParam(
                    UISParamConstants.Q_PROVIDER_ID, "TEL").queryParam(
                    UISParamConstants.Q_RECORD_ID, recordId).request().get();
            getCloudIdTime += (System.nanoTime() - localTime);

            if (resp.getStatus() == Status.OK.getStatusCode()) {
                cloudId = resp.readEntity(CloudId.class);
            } else {
                localTime = System.nanoTime();
                resp = client.target(baseUrl + UIS_PREFIX + "/cloudIds").queryParam(
                        UISParamConstants.Q_PROVIDER_ID, "TEL").queryParam(
                        UISParamConstants.Q_RECORD_ID, recordId).request().post(null);
                createCloudIdTime += (System.nanoTime() - localTime);

                if (resp.getStatus() == Status.OK.getStatusCode()) {
                    cloudId = resp.readEntity(CloudId.class);
                } else {
                    System.out.println("Could not create cloud id for '" + recordId + "'!");
                    continue;
                }
            }
            if (cloudId != null) {
                continue;
            }

            if (i % 5000 == 0) {
                System.out.println("Time overall: '" +
                                   ((System.nanoTime() - startTime) / 1000000000) + "' sec");
                System.out.println("Time get cloudId: '" + (getCloudIdTime / 1000000000) + "' sec");
                System.out.println("Time create cloudId: '" + (createCloudIdTime / 1000000000) +
                                   "' sec");
            }
        }

        System.out.println("Time overall: '" + ((System.nanoTime() - startTime) / 1000000000) +
                           "' sec");
        System.out.println("Time get cloudId: '" + (getCloudIdTime / 1000000000) + "' sec");
        System.out.println("Time create cloudId: '" + (createCloudIdTime / 1000000000) + "' sec");
    }
}
