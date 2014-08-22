package eu.europeana.cloud.integration;

import com.google.gson.Gson;
import eu.europeana.cloud.common.model.CloudId;
import eu.europeana.cloud.common.model.DataProviderProperties;
import eu.europeana.cloud.common.model.DataSet;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.response.ResultSlice;
import eu.europeana.cloud.common.web.ParamConstants;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Iterator;
import java.util.List;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Form;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.ClientProperties;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.jettison.JettisonFeature;
import org.glassfish.jersey.jsonp.JsonProcessingFeature;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.glassfish.jersey.moxy.json.MoxyJsonFeature;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.springframework.util.StopWatch;

/**
 * This is a performance test to ingest a sample dataset. It works in similar
 * fashion as RestIngestionTool
 *
 * @author la4227 <lucas.anastasiou@open.ac.uk>
 */
public class OUDatasetPerformanceTest {

    private static final String MCS_PREFIX = "mcs";//ecloud-service-mcs-rest-0.2-SNAPSHOT";
    private static final String UIS_PREFIX = "uis";//ecloud-service-uis-rest-0.2-SNAPSHOT";

    private static Client client;

    @Option(name = "-e", aliases = {"--endpoint"}, usage = "Endpoint of eCloud services")
    private static String SERVICES_ENDPOINT;

    @Option(name = "-x", aliases = {"--proxy"}, usage = "Proxy to go through when accessing eCloud services", required = false)
    private static String HTTP_PROXY;

    @Option(name = "-dpp", aliases = {"--data-provider-properties"}, usage = "Data provider properties as json", required = false)
    private static String dataProviderPropertiesJson;

    @Option(name = "-p", aliases = {"--provider", "--provider-id"}, usage = "Provider for which a dataset should be ingested")
    private static String providerId;

    @Option(name = "-d", aliases = {"--dataset", "--dataset-id"}, usage = "Ingested dataset id")
    private static String datasetId;

    @Option(name = "-dd", aliases = {"--dataset-description"}, usage = "Ingested dataset description", required = false)
    private static String datasetDescription;

    @Option(name = "-f", aliases = {"--directory", "--folder"}, usage = "Directory with the dataset to be ingested")
    private static String directory;// = "/Users/lucasanastasiou/eCloud/sample-dataset/85";

    public OUDatasetPerformanceTest() {
        //create the proxy-enabled client
        ClientConfig cc = new ClientConfig();
        cc.property(ClientProperties.PROXY_URI, HTTP_PROXY);
        cc.property(ClientProperties.PROXY_USERNAME, "");
        cc.property(ClientProperties.PROXY_PASSWORD, "");
        client = ClientBuilder.newClient(cc);
        client.register(JettisonFeature.class);
        client.register(JacksonFeature.class);
        client.register(MoxyJsonFeature.class);
        client.register(MultiPartFeature.class);
        client.register(JsonProcessingFeature.class);

    }

    public void ingest() {

        // parse data provider properties
        Gson gson = new Gson();
        DataProviderProperties dp = gson.fromJson(dataProviderPropertiesJson, DataProviderProperties.class);

        //
        // (I) create the provider
        //
        Response createProviderResp = createProvider(providerId, dp);
        if (createProviderResp.getStatus() == Response.Status.OK.getStatusCode()) {
            System.out.println("Provider '" + providerId + "' has been created!");
        } else {
            System.out.println("Provider '" + providerId + "' exists!");
        }

        //
        // (II) create dataset
        //
        Response createDatasetResp = createDataset(providerId, datasetId, datasetDescription);

        if (createDatasetResp.getStatus() == Status.CREATED.getStatusCode()) {
            System.out.println("Dataset " + datasetId + " succesfully created.");
        } else {
            System.out.println("Could not create dataset " + datasetId + " ,response:" + createDatasetResp.toString());
        }

        //*
        Iterator<java.io.File> directoryIterator = FileUtils.iterateFiles(new java.io.File(directory), null, true);

        StopWatch taskWatch = new StopWatch(getClass().getSimpleName() + " seperate task watch");
        StopWatch recordWatch = new StopWatch(getClass().getSimpleName() + " record creation watch");
        long totalGetCloudidCounterMillis = 0;
        long totalCreateRepresentationCounterMillis = 0;
        long totalUploadFileCounterMillis = 0;

        while (directoryIterator.hasNext()) {
            File file = directoryIterator.next();

            String filename = file.getName();

            // filename in the form of <id>_<representation format>_v<version>.<extension>
            // e.g. 123_pdf_v1.pdf
            //      345_oai-phm_v2.xml
            String[] filenameSplits = filename.split("_");
            String localId = filenameSplits[0];
            String representationName = filenameSplits[1];
            int version = Integer.parseInt(filenameSplits[2].split("\\.")[0].substring(1));

            //
            // 1. get cloud id : GET/POST /UIS/cloudIds
            //
            recordWatch.start("record creation of local file : " + filename);
            taskWatch.start("Get or create cloud id for localId : " + localId);

            CloudId cloudId = getOrCreateCloudId(providerId, localId);

            taskWatch.stop();
            long millis = taskWatch.getLastTaskTimeMillis();
            totalGetCloudidCounterMillis += millis;

            //
            // 2. create new representatyion version POST /MCS/records/<cloudid>/representations/<representationName>
            //
            taskWatch.start("Create representation");

            Response pushRepresentationResp = pushRepresentation(cloudId, representationName, providerId);

            taskWatch.stop();
            millis = taskWatch.getLastTaskTimeMillis();
            totalCreateRepresentationCounterMillis += millis;

            Representation representation;
            if (pushRepresentationResp.getStatus() == Status.CREATED.getStatusCode()) {
                Response pushRepresentationResp2 = client.target(pushRepresentationResp.getLocation()).request().get();
                if (pushRepresentationResp2.getStatus() == Status.OK.getStatusCode()) {
                    representation = pushRepresentationResp2.readEntity(Representation.class);
                } else {
                    System.out
                            .println("Could not retrieve newly created representation for '" + cloudId.getId() + "'!");
                    continue;
                }
            } else {
                System.out.println("Could not create representation for '" + cloudId.getId() + "'!");
                continue;
            }

            //
            // 3. upload file
            //
            taskWatch.start("Upload file version");
            Response uploadFileResp = uploadFileOperation(file, representation);

            taskWatch.stop();
            millis = taskWatch.getLastTaskTimeMillis();
            totalUploadFileCounterMillis += millis;

            //
            // 4. persist representation
            //
            taskWatch.start("Persist representation");

            Response persistResp2 = persistFileOperation(representation);

            taskWatch.stop();

            //
            // 5. Assign representation to dataset
            //
            taskWatch.start("Assign representation to dataset");

            Response assignResp = assignRepresentationIntoDataset(providerId, datasetId, representation);

            taskWatch.stop();

            recordWatch.stop();

        }

        System.out.println(taskWatch.prettyPrint());
        System.out.println(recordWatch.prettyPrint());
        System.out.println("Get or create cloud ids took in total : " + totalGetCloudidCounterMillis);
        System.out.println("Create record representation took in total : " + totalCreateRepresentationCounterMillis);

        //*/
    }

    public CloudId getOrCreateCloudId(String providerId, String localId) {

        Response resp = client.target(SERVICES_ENDPOINT + UIS_PREFIX + "/cloudIds")
                .queryParam("providerId", providerId)
                .queryParam("recordId", localId)
                .request()
                .get();

        if (resp.getStatus() == Status.OK.getStatusCode()) {
            // cloud id exists already
            return resp.readEntity(CloudId.class);
        } else {
            // invoke the generation of a cloud identifier
            resp = client.target(SERVICES_ENDPOINT + UIS_PREFIX + "/cloudIds")
                    .queryParam("providerId", providerId)
                    .queryParam("recordId", localId)
                    .request()
                    // an empty (but not null) body is required otherwise our proxy cuts this out
                    .post(Entity.json(""));
            if (resp.getStatus() == Status.OK.getStatusCode()) {
                return resp.readEntity(CloudId.class);
            } else {
                return null;
            }
        }
    }

    public Response pushRepresentation(CloudId cloudId, String representationName, String providerId) {
        String representationUrl = SERVICES_ENDPOINT + MCS_PREFIX + "/records/" + cloudId.getId() + "/representations/" + representationName;
        Form form = new Form();
        form.param("providerId", providerId);
        Response response = client.target(representationUrl)
                .request()
                .post(Entity.entity(form, MediaType.APPLICATION_FORM_URLENCODED_TYPE));

        return response;
    }

    public Response uploadFileOperation(java.io.File input, Representation representation) {
        eu.europeana.cloud.common.model.File cloudFile = new eu.europeana.cloud.common.model.File();
        cloudFile.setFileName(input.getName());
        cloudFile.setMimeType(FilenameUtils.getExtension(input.getAbsolutePath()));
        cloudFile.setMimeType("text");
        byte[] content = null;
        try {
            content = FileUtils.readFileToByteArray(input);
        } catch (IOException ex) {
            System.out.println(ex.getMessage());
        }

//        System.out.println(content.length + " bytes ");
        FormDataMultiPart multipart = new FormDataMultiPart()
                .field(ParamConstants.F_FILE_MIME, cloudFile.getMimeType())
                .field(ParamConstants.F_FILE_DATA, new ByteArrayInputStream(content),
                        MediaType.APPLICATION_OCTET_STREAM_TYPE);

        String targetUrl = SERVICES_ENDPOINT + MCS_PREFIX
                + "/records/" + representation.getCloudId()
                + "/representations/" + representation.getRepresentationName()
                + "/versions/" + representation.getVersion()
                + "/files/";

        Response resp = client
                .target(targetUrl)
                .request()
                .post(Entity.entity(multipart, multipart.getMediaType()));

        System.out.println("resp : " + resp);
        return resp;
    }

    public Response persistFileOperation(Representation representation) {
        String targetUrl = SERVICES_ENDPOINT + MCS_PREFIX
                + "/records/" + representation.getCloudId() + "/representations/"
                + representation.getRepresentationName() + "/versions/" + representation.getVersion()
                + "/persist";
        Response resp = client
                .target(targetUrl).request()
                // an empty (but not null) body is required otherwise our proxy cuts this out
                .post(Entity.json(""));
        System.out.println("persist: " + resp.toString()
        );
        return resp;
    }

    public Response assignRepresentationIntoDataset(String providerId, String dataSetId, Representation representation) {
        String targetUrl = SERVICES_ENDPOINT + MCS_PREFIX
                + "/data-providers/" + providerId + "/data-sets/" + dataSetId
                + "/assignments/";
        Form form = new Form();
        form.param("cloudId", representation.getCloudId());
        form.param("representationName", representation.getRepresentationName());
        form.param("version", representation.getVersion());
        Response resp = client
                .target(targetUrl)
                .request()
                .post(Entity.entity(form, MediaType.APPLICATION_FORM_URLENCODED_TYPE));

        System.out.println(resp.toString());
        return resp;
    }

    public Response createDataset(String dataProviderId, String datasetId, String description) {
        String targetUrl = SERVICES_ENDPOINT + MCS_PREFIX
                + "/data-providers/" + dataProviderId + "/data-sets/";

        Form form = new Form();
        form.param("dataSetId", datasetId);
        form.param("description", description);
        Response resp = client.target(targetUrl)
                .request()
                .post(Entity.entity(form, MediaType.APPLICATION_FORM_URLENCODED_TYPE));
        return resp;
    }

    public Response createProvider(String providerId, DataProviderProperties dp) {
        String targetUrl = SERVICES_ENDPOINT + UIS_PREFIX + "/data-providers/";

        Response createProviderResp = client.target(targetUrl)
                .queryParam("providerId", providerId)
                .request()
                .post(Entity.json(dp));

        return createProviderResp;
    }

    private void deleteProviderDatasets(String providerId) {
        List<DataSet> datasets = getProvidersDatasets(providerId);

        int count = 0;
        for (DataSet dataSet : datasets) {
            deleteDataSetAndRecords(dataSet);
            //delete only first dataset
            if (count == 0) {
                break;
            }
        }
    }

    private List<DataSet> getProvidersDatasets(String providerId) {
        // get provider's datasets
        String targetUrl = SERVICES_ENDPOINT + MCS_PREFIX + "/data-providers/" + providerId + "/data-sets/";
        Response resp = client.target(targetUrl)
                //                .queryParam("startFrom", 0)
                .request()
                .get();

        ResultSlice<DataSet> datasets = null;
        if (resp.getStatus() == Status.OK.getStatusCode()) {
            datasets = resp.readEntity(ResultSlice.class);
        } else {
            System.out.println("Could read provider's : " + providerId + " datasets");
            return null;
        }
        return datasets.getResults();

    }

    /**
     * Deletes record with all its representations in all versions
     *
     * @param cloudId
     */
    private void deleteRecord(String cloudId) {
        String targetUrl = SERVICES_ENDPOINT + MCS_PREFIX + "/records/" + cloudId;
        Response resp = client.target(targetUrl)
                .request()
                .delete();

        if (resp.getStatus() == Status.NO_CONTENT.getStatusCode()) {
            System.out.println("Record " + cloudId + " succesfully deleted (with all representations and versions)");
        } else {
            System.out.println("Could not delete " + cloudId + " : " + resp.getStatus() + " : " + resp.toString());
        }
    }

    private void deleteDataset(DataSet dataSet) {
        String targetUrl = SERVICES_ENDPOINT + MCS_PREFIX + "data-providers/" + dataSet.getProviderId() + "/data-sets/" + dataSet.getId() + "/";
        Response resp = client.target(targetUrl).request().delete();

        if (resp.getStatus() == Status.NO_CONTENT.getStatusCode()) {
            System.out.println("Dataset " + dataSet.getId() + " succesfully deleted.");
        } else {
            System.out.println("Dataset " + dataSet.getId() + " deletion failed..");
            System.out.println(dataSet.toString());
            System.out.println(resp.toString());
        }
    }

    /**
     * Delete all dataset's representations and dataset itself
     *
     * @param dataSet
     */
    private void deleteDataSetAndRecords(DataSet dataSet) {

        //
        // delete all records of dataset
        //
        List<Representation> datasetRecords = getDatasetRecords(dataSet);
        for (Representation representation : datasetRecords) {
            deleteRecord(representation.getCloudId());
        }
        //
        // delete dataset
        //
        deleteDataset(dataSet);
    }

    private List<Representation> getDatasetRecords(DataSet dataSet) {
        String targetUrl = SERVICES_ENDPOINT + MCS_PREFIX + "/data-providers/" + dataSet.getProviderId() + "/data-sets/" + dataSet.getId() + "/";
        System.out.println(targetUrl);
        Response resp = client.target(targetUrl)
                .request()
                .get();

        ResultSlice<Representation> records = null;
        if (resp.getStatus() == Status.OK.getStatusCode()) {
            records = resp.readEntity(ResultSlice.class);
        } else {
            System.out.println("Could read dataSet's : " + dataSet.getId() + " representations...");
            System.out.println(resp.toString());
            return null;
        }

        for (Representation repr : records.getResults()) {
            System.out.println(repr);
        }
        return records.getResults();
    }

    public static DigestInputStream md5InputStream(InputStream is) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            return new DigestInputStream(is, md);
        } catch (NoSuchAlgorithmException ex) {
            throw new AssertionError("Cannot get instance of MD5 but such algorithm should be provided", ex);
        }
    }

    public static void main(String args[]) {

        for (int i = 0; i < args.length; i++) {
            String string = args[i];
            System.out.println("args[" + i + "] : " + string);
        }

        OUDatasetPerformanceTest ouPerfTest = new OUDatasetPerformanceTest();
        CmdLineParser parser = new CmdLineParser(ouPerfTest);
        try {
            parser.parseArgument(args);
            ouPerfTest.ingest();
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
