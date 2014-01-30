/* IngestionTool.java - created on Jan 20, 2014, Copyright (c) 2011 The European Library, all rights reserved */
package eu.europeana.cloud.integration;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Iterator;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Form;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.apache.commons.io.FileUtils;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.jettison.JettisonFeature;
import org.glassfish.jersey.jsonp.JsonProcessingFeature;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.glassfish.jersey.moxy.json.MoxyJsonFeature;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.springframework.stereotype.Component;

import com.google.common.io.ByteStreams;

import eu.europeana.cloud.common.model.CloudId;
import eu.europeana.cloud.common.model.DataProviderProperties;
import eu.europeana.cloud.common.model.File;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.response.ResultSlice;
import eu.europeana.cloud.common.web.ParamConstants;

/**
 * This is a command line tool to ingest a new data set.
 * 
 * @author Markus Muhr (markus.muhr@kb.nl)
 * @since Jan 20, 2014
 */
@Component
public class RestIngestionTool {
    /**
     * Available operations for this tool.
     * 
     * @author Markus Muhr (markus.muhr@kb.nl)
     * @since Jan 19, 2012
     */
    public enum Operation {
        /**
         * ingests data set
         */
        ingestDataSet(
                      "Ingests a data set for a given provider-id, dataset-id, schema and directory!"),
        /**
         * reads data set
         */
        readDataSet("Reads a data set for a given provider-id, dataset-id, schema and directory!"),
        /**
         * deletes data set
         */
        deleteDataSet(
                      "Deletes a data set for a given provider-id, dataset-id, schema and directory!");

        /**
         * description for option
         */
        private String description;

        /**
         * Creates a new instance of this class.
         * 
         * @param description
         */
        private Operation(String description) {
            this.description = description;
        }

        /**
         * @return description for option
         */
        public String getDescription() {
            return description;
        }
    }

    @Option(name = "-o", aliases = { "--operation" }, required = true)
    private Operation           operation;

    @Option(name = "-p", aliases = { "--provider" }, usage = "Provider for which a dataset should be ingested")
    private String              providerId;

    @Option(name = "-d", aliases = { "--dataset" }, usage = "Dataset for which the data should be ingested")
    private String              dataSetId;

    @Option(name = "-s", aliases = { "--schema" }, usage = "Schema specifying the format of the dataset!")
    private String              schema;

    @Option(name = "-f", aliases = { "--file" }, metaVar = "DIR", usage = "Directory with files to be ingested!")
    private String              directory;

    /**
     * client configuration
     */
    private Client              client;
    /**
     * provides base url for rest api
     */
    private String              baseUrl;

    /**
     * Prefix of UIS based rest calls.
     */
    private static final String UIS_PREFIX = "ecloud-service-uis-rest-0.1-SNAPSHOT";

    /**
     * Prefix of MCS based rest calls.
     */
    private static final String MCS_PREFIX = "ecloud-service-mcs-rest-0.1-SNAPSHOT";

    /**
     * Creates a new instance of this class.
     */
    public RestIngestionTool() {
        client = ClientBuilder.newBuilder().register(JettisonFeature.class).register(
                JacksonFeature.class).register(MoxyJsonFeature.class).register(
                MultiPartFeature.class).register(JsonProcessingFeature.class).build();
        baseUrl = "http://ecloud.eanadev.org:8080/";
// Properties props = new Properties();
// try {
// props.load(new FileInputStream(new java.io.File("src/main/resources/client.properties")));
// baseUrl = props.getProperty("server.baseUrl");
// } catch (IOException e) {
// e.printStackTrace();
// }
    }

    /**
     * Runs operations for this tool.
     */
    private void run() {
        switch (operation) {
        case ingestDataSet:
            try {
                ingestDataSet();
            } catch (Exception e) {
                throw new RuntimeException("Could not ingest dataset!", e);
            }
            break;
        case readDataSet:
            try {
                readDataSet();
            } catch (Exception e) {
                throw new RuntimeException("Could not read dataset!", e);
            }
            break;
        case deleteDataSet:
            try {
                deleteDataSet();
            } catch (Exception e) {
                throw new RuntimeException("Could not delete dataset!", e);
            }
            break;
        }
    }

    /**
     * Ingestion of a data set.
     * 
     * @throws Exception
     */
    @SuppressWarnings("resource")
    private void ingestDataSet() throws Exception {
        long startTime = System.nanoTime();

        DataProviderProperties dp = new DataProviderProperties(providerId, "", "", "", "", "",
                "Ingesion Tool Provider", "");
        Response resp = client.target(baseUrl + UIS_PREFIX + "/uniqueId/data-providers").queryParam(
                "providerId", providerId).request().post(Entity.json(dp));
        if (resp.getStatus() == Status.OK.getStatusCode()) {
            System.out.println("Provider '" + providerId + "' has been created!");
        } else {
            System.out.println("Provider '" + providerId + "' exists!");
        }

        System.out.println("Provider setup: '" + ((System.nanoTime() - startTime) / 1000000) +
                           "' msec");
        startTime = System.nanoTime();

        resp = client.target(baseUrl + MCS_PREFIX + "/data-providers/" + providerId + "/data-sets").request().post(
                Entity.form(new Form("dataSetId", dataSetId).param("description",
                        "Ingestion Tool Dataset")));
        if (resp.getStatus() == Status.OK.getStatusCode()) {
            System.out.println("Dataset '" + dataSetId + "' has been created!");
        } else {
            System.out.println("Dataset '" + dataSetId + "' exists!");
        }

        System.out.println("Dataset setup: '" + ((System.nanoTime() - startTime) / 1000000) +
                           "' msec");
        startTime = System.nanoTime();

        long cloudIdTime = 0;
        long representationTime = 0;
        long uploadTime = 0;
        long persistTime = 0;
        long assignTime = 0;

        int scheduled = 0;
        int done = 0;
        Iterator<java.io.File> iter = FileUtils.iterateFiles(new java.io.File(directory), null,
                true);
        while (iter.hasNext()) {
            scheduled++;

            java.io.File input = iter.next();

            File file = new File();
            file.setFileName(input.getName());
            file.setMimeType("text");

            long localTime = System.nanoTime();
            int endIdx = input.getName().lastIndexOf(".");
            String localId = endIdx > 0 ? input.getName().substring(0, endIdx) : input.getName();
            CloudId cloudId;
            resp = client.target(baseUrl + UIS_PREFIX + "/uniqueId/getCloudId").queryParam(
                    "providerId", providerId).queryParam("recordId", localId).request().get();
            if (resp.getStatus() == Status.OK.getStatusCode()) {
                cloudId = resp.readEntity(CloudId.class);
            } else {
                resp = client.target(baseUrl + UIS_PREFIX + "/uniqueId/createCloudIdLocal").queryParam(
                        "providerId", providerId).queryParam("recordId", localId).request().get();
                if (resp.getStatus() == Status.OK.getStatusCode()) {
                    cloudId = resp.readEntity(CloudId.class);
                } else {
                    System.out.println("Could not create cloud id for '" + localId + "'!");
                    continue;
                }
            }
            cloudIdTime += (System.nanoTime() - localTime);

            localTime = System.nanoTime();
            resp = client.target(
                    baseUrl + MCS_PREFIX + "/records/" + cloudId.getId() + "/representations/" +
                            schema).request().post(
                    Entity.entity(new Form("providerId", providerId),
                            MediaType.APPLICATION_FORM_URLENCODED_TYPE));
            Representation representation;
            if (resp.getStatus() == Status.CREATED.getStatusCode()) {
                resp = client.target(resp.getLocation()).request().get();
                if (resp.getStatus() == Status.OK.getStatusCode()) {
                    representation = resp.readEntity(Representation.class);
                } else {
                    System.out.println("Could not retrieve newly created representation for '" +
                                       cloudId.getId() + "'!");
                    continue;
                }
            } else {
                System.out.println("Could not create representation for '" + cloudId.getId() + "'!");
                continue;
            }
            representationTime += (System.nanoTime() - localTime);

            localTime = System.nanoTime();
            byte[] content = FileUtils.readFileToByteArray(input);
            FormDataMultiPart multipart = new FormDataMultiPart().field(ParamConstants.F_FILE_MIME,
                    file.getMimeType()).field(ParamConstants.F_FILE_DATA,
                    new ByteArrayInputStream(content), MediaType.APPLICATION_OCTET_STREAM_TYPE);
            resp = client.target(
                    baseUrl + MCS_PREFIX + "/records/" + representation.getRecordId() +
                            "/representations/" + representation.getSchema() + "/versions/" +
                            representation.getVersion() + "/files/" + file.getFileName()).request().put(
                    Entity.entity(multipart, multipart.getMediaType()));
            if (resp.getStatus() != Status.CREATED.getStatusCode()) {
                System.out.println("Could not upload file for '" + cloudId.getId() + "'!");
                continue;
            }
            uploadTime += (System.nanoTime() - localTime);

            localTime = System.nanoTime();
            resp = client.target(
                    baseUrl + MCS_PREFIX + "/records/" + representation.getRecordId() +
                            "/representations/" + representation.getSchema() + "/versions/" +
                            representation.getVersion() + "/persist").request().post(null);
            if (resp.getStatus() != Status.CREATED.getStatusCode()) {
                System.out.println("Could not persist representation for '" + cloudId.getId() +
                                   "'!");
                continue;
            }
            persistTime += (System.nanoTime() - localTime);

            localTime = System.nanoTime();
            resp = client.target(
                    baseUrl + MCS_PREFIX + "/data-providers/" + providerId + "/data-sets/" +
                            dataSetId + "/assignments").request().post(
                    Entity.form(new Form("recordId", representation.getRecordId()).param("schema",
                            representation.getSchema())));
            if (resp.getStatus() != Status.NO_CONTENT.getStatusCode()) {
                System.out.println("Could not assign representation for '" + cloudId.getId() + "'!");
                continue;
            }
            assignTime += (System.nanoTime() - localTime);

            done++;

            if (scheduled % 10 == 0) {
                System.out.println("Scheduled: " + scheduled);
                System.out.println("Done: " + done);
                System.out.println("Time overall: '" +
                                   ((System.nanoTime() - startTime) / 1000000000) + "' sec");
                System.out.println("Time cloudId: '" + (cloudIdTime / 1000000000) + "' sec");
                System.out.println("Time representation: '" + (representationTime / 1000000000) +
                                   "' sec");
                System.out.println("Time upload: '" + (uploadTime / 1000000000) + "' sec");
                System.out.println("Time persist: '" + (persistTime / 1000000000) + "' sec");
                System.out.println("Time assign: '" + (assignTime / 1000000000) + "' sec");
            }
        }

        System.out.println("Scheduled: " + scheduled);
        System.out.println("Done: " + done);
    }

    /**
     * Reads a data set to a directory!
     * 
     * @throws Exception
     */
    @SuppressWarnings("unchecked")
    private void readDataSet() throws Exception {
        Response resp = client.target(
                baseUrl + MCS_PREFIX + "/data-providers/" + providerId + "/data-sets/" + dataSetId).request().get();
        ResultSlice<Representation> dataset;
        if (resp.getStatus() == Status.OK.getStatusCode()) {
            dataset = resp.readEntity(ResultSlice.class);
        } else {
            return;
        }

        int scheduled = 0;
        int done = 0;
        for (Representation representation : dataset.getResults()) {
            scheduled++;

            resp = client.target(
                    baseUrl + MCS_PREFIX + "/records/" + representation.getRecordId() +
                            "/representations/" + representation.getSchema() + "/versions/" +
                            representation.getVersion() + "/files/" +
                            representation.getFiles().get(0).getFileName()).request().get();
            if (resp.getStatus() == Status.OK.getStatusCode()) {
                InputStream responseStream = resp.readEntity(InputStream.class);
                byte[] responseContent = ByteStreams.toByteArray(responseStream);
                FileUtils.writeByteArrayToFile(new java.io.File(
                        directory + "/" + representation.getFiles().get(0).getFileName()),
                        responseContent);
            } else {
                continue;
            }

            done++;

            if (scheduled % 100 == 0) {
                System.out.println("Scheduled: " + scheduled);
                System.out.println("Done: " + done);
            }
        }
        System.out.println("Scheduled: " + scheduled);
        System.out.println("Done: " + done);
    }

    /**
     * Delete a data set!
     * 
     * @throws Exception
     */
    @SuppressWarnings("unchecked")
    private void deleteDataSet() throws Exception {
        Response resp = client.target(
                baseUrl + MCS_PREFIX + "/data-providers/" + providerId + "/data-sets/" + dataSetId).request().get();
        ResultSlice<Representation> dataset;
        if (resp.getStatus() == Status.OK.getStatusCode()) {
            dataset = resp.readEntity(ResultSlice.class);
        } else {
            System.out.println("Could not read dataset '" + dataSetId + "'!");
            return;
        }

        int scheduled = 0;
        int done = 0;
        for (Representation representation : dataset.getResults()) {
            scheduled++;

            resp = client.target(
                    baseUrl + MCS_PREFIX + "/records/" + representation.getRecordId() +
                            "/representations/" + representation.getSchema()).request().delete();
            if (resp.getStatus() != Status.NO_CONTENT.getStatusCode()) {
                System.out.println("Could not delete representation for '" +
                                   representation.getRecordId() + "'!");
                continue;
            }

// resp = client.target(
// baseUrl + MCS_PREFIX + "/data-providers/" + providerId + "/data-sets/" +
// dataSetId + "/assignments").queryParam("recordId",
// representation.getRecordId()).queryParam("schema",
// representation.getSchema()).request().delete();
// if (resp.getStatus() != Status.OK.getStatusCode()) {
// System.out.println("Could not remove assignment of representation for '" +
// representation.getRecordId() + "'!");
// continue;
// }

            done++;

            if (scheduled % 100 == 0) {
                System.out.println("Scheduled: " + scheduled);
                System.out.println("Done: " + done);
            }
        }
        System.out.println("Scheduled: " + scheduled);
        System.out.println("Done: " + done);

        resp = client.target(
                baseUrl + MCS_PREFIX + "/data-providers/" + providerId + "/data-sets/" + dataSetId).request().delete();
        if (resp.getStatus() != Status.OK.getStatusCode()) {
            System.out.println("Could not delete dataset '" + dataSetId + "'!");
        }
        System.out.println("Deleted dataset: " + dataSetId);
    }

    /**
     * @param args
     */
    public static void main(String[] args) {
        RestIngestionTool tool = new RestIngestionTool();
        CmdLineParser parser = new CmdLineParser(tool);
        try {
            parser.parseArgument(args);
            tool.run();
        } catch (CmdLineException e) {
            // handling of wrong arguments
            System.err.println(e.getMessage());
            parser.printUsage(System.err);
        } catch (Exception e) {
            System.err.println(e.getMessage());
            e.printStackTrace(System.err);
        }
    }
}
