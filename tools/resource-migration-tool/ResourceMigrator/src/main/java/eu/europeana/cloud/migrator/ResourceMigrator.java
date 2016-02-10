package eu.europeana.cloud.migrator;

import eu.europeana.cloud.client.uis.rest.CloudException;
import eu.europeana.cloud.client.uis.rest.UISClient;
import eu.europeana.cloud.common.model.CloudId;
import eu.europeana.cloud.common.model.DataProviderProperties;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.mcs.driver.FileServiceClient;
import eu.europeana.cloud.mcs.driver.RecordServiceClient;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import eu.europeana.cloud.service.mcs.exception.ProviderNotExistsException;
import eu.europeana.cloud.service.mcs.exception.RecordNotExistsException;
import eu.europeana.cloud.service.mcs.exception.RepresentationNotExistsException;
import eu.europeana.cloud.service.uis.exception.ProviderAlreadyExistsException;
import eu.europeana.cloud.service.uis.exception.RecordExistsException;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;

import javax.ws.rs.ProcessingException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.*;
import java.nio.charset.Charset;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;

public class ResourceMigrator {

    public static final String LINUX_SEPARATOR = "/";

    public static final String WINDOWS_SEPARATOR = "\\";

    private static final Logger logger = Logger.getLogger(ResourceMigrator.class);

    private static final Map<String, String> mimeTypes = new HashMap<String, String>();

    static {
        mimeTypes.put("jp2", "image/jp2");
        mimeTypes.put("jpg", "image/jpeg");
        mimeTypes.put("jpeg", "image/jpeg");
        mimeTypes.put("tif", "image/tiff");
        mimeTypes.put("tiff", "image/tiff");
        mimeTypes.put("xml", "text/xml");
    }

    // default number of retries
    private static final int DEFAULT_RETRIES = 10;

    // Default threads pool size
    private static final byte DEFAULT_POOL_SIZE = 10;

    // Pool of threads used to migrate files
    private static final ExecutorService threadPool = Executors
            .newFixedThreadPool(DEFAULT_POOL_SIZE);

    /**
     * MCS java client
     */
    @Autowired
    private RecordServiceClient mcs;

    /**
     * UIS java client
     */
    @Autowired
    private UISClient uis;

    /**
     * MCS files java client
     */
    @Autowired
    private FileServiceClient fsc;

    /**
     * Resource provider, specific implementation will be used in runtime, eg. FoodAndDrinkResourceProvider
     */
    private ResourceProvider resourceProvider;

    public ResourceMigrator(ResourceProvider resProvider) {
        resourceProvider = resProvider;
    }

    public boolean migrate(boolean clean) {
        boolean success = true;

        long start = System.currentTimeMillis();

        // key is provider id, value is a list of files to add
        Map<String, List<FilePaths>> paths = resourceProvider.scan();

        if (logger.isDebugEnabled()) {
            for (Map.Entry<String, List<FilePaths>> entry : paths.entrySet()) {
                for (FilePaths fp : entry.getValue()) {
                    logger.debug("Found paths for provider " + entry.getKey() + " in location " + fp.getLocation() + " (" + fp.getFullPaths().size() + "):");
                    for (String s : fp.getFullPaths())
                        logger.debug(s);
                }
            }
        }

        logger.info("Scanning resource provider locations finished in " + String.valueOf(((float) (System.currentTimeMillis() - start) / (float) 1000)) + " sec.");
        List<Future<MigrationResult>> results = null;
        List<Callable<MigrationResult>> tasks = new ArrayList<Callable<MigrationResult>>();

        // create task for each provider
        for (String providerId : paths.keySet()) {
            if (clean) {
                logger.info("Cleaning " + providerId);
                clean(providerId);
            }
            logger.info("Starting task thread for provider " + providerId + "...");
            tasks.add(new ProviderMigrator(providerId, paths.get(providerId)));
            break;
        }

        try {
            // invoke a separate thread for each provider
            results = threadPool.invokeAll(tasks);

            MigrationResult providerResult;
            for (Future<MigrationResult> result : results) {
                providerResult = result.get();
                logger.info("Migration of provider " + providerResult.getProviderId() + " performed " + (providerResult.isSuccessful() ? "" : "un") + "successfully. Migration time: " + providerResult.getTime() + " sec.");
                success &= providerResult.isSuccessful();
            }
        } catch (InterruptedException e) {
            logger.error("Migration processed interrupted.", e);
        } catch (ExecutionException e) {
            logger.error("Problem with migration task thread execution.", e);
        }

        return success;
    }

    /**
     * Create representation for the specified provider and cloud identifier. Representation name is supplied by the resource provider.
     * If there is already a representation and it is persistent then we cannot add anything to this record. If there are non persistent
     * representations they will be removed. Please, be careful when calling this method.
     *
     * @param providerId data provider identifier
     * @param cloudId    cloud identifier
     * @return URI of the created representation and version
     */
    private URI createRepresentationName(String providerId, String cloudId) {
        int retries = DEFAULT_RETRIES;
        while (retries-- > 0) {
            try {
                // get all representations
                List<Representation> representations;

                try {
                    representations = mcs.getRepresentations(cloudId, resourceProvider.getRepresentationName());
                } catch (RepresentationNotExistsException e) {
                    // when there are no representations add a new one
                    return mcs.createRepresentation(cloudId, resourceProvider.getRepresentationName(), providerId);
                }

                // when there are some old representations it means that somebody had to add them, delete non persistent ones
                for (Representation representation : representations) {
                    if (representation.isPersistent())
                        return null;
                    else
                        mcs.deleteRepresentation(cloudId, resourceProvider.getRepresentationName(), representation.getVersion());
                }
                // when there are no representations add a new one
                return mcs.createRepresentation(cloudId, resourceProvider.getRepresentationName(), providerId);
            } catch (ProcessingException e) {
                logger.warn("Error processing HTTP request while creating representation for record " + cloudId + ", provider " + providerId + ". Retries left: " + retries, e);
            } catch (ProviderNotExistsException e) {
                logger.error("Provider " + providerId + " does not exist!");
                break;
            } catch (RecordNotExistsException e) {
                logger.error("Record " + cloudId + " does not exist!");
                break;
            } catch (MCSException e) {
                logger.error("Problem with creating representation name!");
                break;
            }
        }
        return null;
    }

    /**
     * Create new record for specified provider and local identifier. If record already exists return its identifier.
     *
     * @param providerId data provider identifier
     * @param localId    local identifier of the record
     * @return newly created cloud identifier or existing cloud identifier
     */
    private String createRecord(String providerId, String localId) {
        int retries = DEFAULT_RETRIES;
        while (retries-- > 0) {
            try {
                CloudId cloudId = uis.createCloudId(providerId, localId);
                if (cloudId != null)
                    return cloudId.getId();
            } catch (ProcessingException e) {
                logger.warn("Error processing HTTP request while creating record for provider " + providerId + " and local id " + localId + ". Retries left: " + retries, e);
            } catch (CloudException e) {
                if (e.getCause() instanceof RecordExistsException) {
                    try {
                        return uis.getCloudId(providerId, localId).getId();
                    } catch (ProcessingException e1) {
                        logger.warn("Error processing HTTP request while creating record for provider " + providerId + ". Retries left: " + retries, e);
                    } catch (CloudException e1) {
                        logger.warn("Record for provider " + providerId + " with local id " + localId + " could not be created", e1);
                        break;
                    }
                } else {
                    logger.warn("Record for provider " + providerId + " with local id " + localId + " could not be created", e);
                    break;
                }
            }
        }
        return null;
    }

    /**
     * Creates file name in ECloud.
     *
     * @param path     path to file, if location is remote it must have correct URI syntax, otherwise it must be a proper path in a filesystem
     * @param version  version string, must be appropriate for the given record
     * @param recordId global record identifier
     */
    private URI createFilename(String path, String version, String recordId) {
        String mimeType = "";
        InputStream is = null;
        URI fullURI = null;
        try {
            if (resourceProvider.isLocal()) {
                try {
                    File localFile = new File(path);
                    fullURI = localFile.toURI();
                    mimeType = Files.probeContentType(localFile.toPath());
                } catch (IOException e) {
                    mimeType = mimeFromExtension(path);
                }
            } else {
                fullURI = new URI(path);
                mimeType = getContentType(fullURI.toURL());
            }
            if (mimeType == null)
                mimeType = mimeFromExtension(path);
            if (fullURI == null) {
                logger.error("URI for path " + path + " could not be created.");
                return null;
            }
        } catch (URISyntaxException e) {
            logger.error(path + " is not correct URI", e);
            return null;
        } catch (IOException e) {
            logger.error("Problem with file: " + path, e);
            return null;
        }

        int retries = DEFAULT_RETRIES;
        while (retries-- > 0) {
            try {
                is = fullURI.toURL().openStream();

                // filename is retrieved from URI instead of path in order to contain proper slash characters ("/")
                return fsc.uploadFile(recordId, resourceProvider.getRepresentationName(), version, resourceProvider.getFilename(fullURI.toString()), is, mimeType);
            } catch (ProcessingException e) {
                logger.warn("Processing HTTP request failed. Upload file: " + resourceProvider.getFilename(fullURI.toString()) + " for record id: " + recordId + ". Retries left: " + retries);
            } catch (SocketTimeoutException e) {
                logger.warn("Read time out. Upload file: " + resourceProvider.getFilename(fullURI.toString()) + " for record id: " + recordId + ". Retries left: " + retries);
            } catch (ConnectException e) {
                logger.error("Connection timeout. Upload file: " + resourceProvider.getFilename(fullURI.toString()) + " for record id: " + recordId + ". Retries left: " + retries);
            } catch (FileNotFoundException e) {
                logger.error("Could not open input stream to file!", e);
            } catch (IOException e) {
                logger.error("Problem with detecting mime type from file (" + path + ")", e);
            } catch (MCSException e) {
                logger.error("ECloud error when uploading file.", e);
            } finally {
                try {
                    is.close();
                } catch (IOException e) {
                    logger.warn("Could not close stream.", e);
                }
            }
        }
        return null;
    }


    /**
     * Http HEAD Method to get URL content type
     *
     * @param url
     * @return content type
     * @throws IOException
     */
    private String getContentType(URL url) throws IOException {
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setRequestMethod("HEAD");
        if (isRedirect(connection.getResponseCode())) {
            String newUrl = connection.getHeaderField("Location"); // get redirect url from "location" header field
            logger.warn("Original request URL: " + url.toString() + " redirected to: " + newUrl);
            return getContentType(new URL(newUrl));
        }
        return connection.getContentType();
    }

    /**
     * Check status code for redirects
     *
     * @param statusCode
     * @return true if matched redirect group
     */
    private boolean isRedirect(int statusCode) {
        return (statusCode != HttpURLConnection.HTTP_OK && (statusCode == HttpURLConnection.HTTP_MOVED_TEMP
                || statusCode == HttpURLConnection.HTTP_MOVED_PERM
                || statusCode == HttpURLConnection.HTTP_SEE_OTHER));
    }

    /**
     * Detects mime type according to the file extension.
     *
     * @param path
     * @return
     */
    private String mimeFromExtension(String path) {
        int i = path.lastIndexOf(".");
        if (i == -1)
            return "application/octet-stream";
        return mimeTypes.get(path.substring(i + 1));
    }

    public String createProvider(String path) {
        String providerId = resourceProvider.getProviderId(path);
        DataProviderProperties providerProps = resourceProvider.getDataProviderProperties(path);

        int retries = DEFAULT_RETRIES;
        while (retries-- > 0) {
            try {
                uis.createProvider(providerId, providerProps);
                return providerId;
            } catch (ProcessingException e) {
                logger.warn("Error processing HTTP request while creating provider " + providerId + ". Retries left: " + retries, e);
            } catch (CloudException e) {
                if (e.getCause() instanceof ProviderAlreadyExistsException) {
                    logger.warn("Provider " + providerId + " already exists.");
                    return providerId;
                }
                logger.error("Exception when creating provider occured.", e);
                break;
            }
        }
        return null;
    }

    private void removeProcessedPaths(String providerId, FilePaths paths) {
        try {
            List<String> processed = new ArrayList<String>();
            for (String line : Files.readAllLines(FileSystems.getDefault().getPath(".", providerId + ".txt"), Charset.forName("UTF-8"))) {
                StringTokenizer st = new StringTokenizer(line, "=");
                if (st.hasMoreTokens()) {
                    processed.add(st.nextToken());
                }
            }
            paths.getFullPaths().removeAll(processed);
        } catch (IOException e) {
            logger.warn("Progress file for provider " + providerId + " could not be opened. Returning all paths.", e);
        }
    }


    private boolean processProvider(String providerId, FilePaths providerPaths) {
        // key is local identifier, value is cloud identifier
        Map<String, String> cloudIds = new HashMap<String, String>();
        // key is local identifier, value is version identifier
        Map<String, String> versionIds = new HashMap<String, String>();
        // key is version identifier, value is a list of strings containing path=URI
        Map<String, List<String>> processed = new HashMap<String, List<String>>();

        // first remove already processed paths, if there is no progress file for the provider no filtering is performed
        removeProcessedPaths(providerId, providerPaths);

        int counter = 0;
        int errors = 0;

        if (providerPaths.size() > 0) {
            // first create provider, one path is enough to determine
            if (createProvider(providerPaths.getFullPaths().get(0)) == null) {
                // when create provider was not successful finish processing
                return false;
            }

            String prevLocalId = null;
            for (String path : providerPaths.getFullPaths()) {
                if ((int) (((float) (counter) / (float) providerPaths.size()) * 100) > (int) (((float) (counter - 1) / (float) providerPaths.size()) * 100))
                    logger.info("Provider: " + providerId + ". Progress: " + counter + " of " + providerPaths.size() + " (" + (int) (((float) (counter) / (float) providerPaths.size()) * 100) + "%). Errors: " + errors);
                counter++;
                // get local record identifier
                String localId = resourceProvider.getLocalIdentifier(providerPaths.getLocation(), path);
                if (localId == null) {
                    // when local identifier is null it means that the path may be wrong
                    logger.error("Local identifier for path: " + path + " could not be obtained. Skipping path...");
                    continue;
                }
                if (!localId.equals(prevLocalId)) {
                    if (prevLocalId != null && cloudIds.get(prevLocalId) != null && versionIds.get(prevLocalId) != null) {
                        // persist previous version if it was created
                        URI persistent = persistVersion(cloudIds.get(prevLocalId), versionIds.get(prevLocalId));
                        if (persistent != null) {
                            if (!permitVersion(cloudIds.get(prevLocalId), versionIds.get(prevLocalId)))
                                logger.warn("Could not grant permissions to version " + versionIds.get(prevLocalId) + " of record " + cloudIds.get(prevLocalId) + ". Version is only available for current user.");
                            saveProgress(providerId, processed.get(versionIds.get(prevLocalId)), false);
                        }
                    }
                    prevLocalId = localId;
                }
                if (cloudIds.get(localId) == null) {
                    // create new record when it was not created before
                    String cloudId = createRecord(providerId, localId);
                    if (cloudId == null) {
                        // this is an error
                        errors++;
                        continue; // skip this path
                    }
                    cloudIds.put(localId, cloudId);
                    // create representation for the record
                    URI uri = createRepresentationName(providerId, cloudId);
                    if (uri == null) {
                        // this is not an error, version is already there and is persistent
                        continue; // skip this path
                    }
                    String verId = getVersionIdentifier(uri);
                    if (verId == null) {
                        // this is an error, version identifier could not be retrieved from representation URI
                        errors++;
                        continue; // skip this path
                    }
                    versionIds.put(localId, verId);
                }

                // create file name in ECloud with the specified name
                URI fileAdded = createFilename(path, versionIds.get(localId), cloudIds.get(localId));
                if (fileAdded == null) {
                    // this is an error, upload failed
                    prevLocalId = null; // this should prevent persisting the version and saving progress
                    continue; // skip this path
                }
                // put the created URI for path to processed list
                if (processed.get(versionIds.get(localId)) == null)
                    processed.put(versionIds.get(localId), new ArrayList<String>());
                processed.get(versionIds.get(localId)).add(path + "=" + fileAdded);
            }
            if (prevLocalId != null) {
                // persist previous version
                URI persistent = persistVersion(cloudIds.get(prevLocalId), versionIds.get(prevLocalId));
                if (persistent != null) {
                    if (!permitVersion(cloudIds.get(prevLocalId), versionIds.get(prevLocalId)))
                        logger.warn("Could not grant permissions to version " + versionIds.get(prevLocalId) + " of record " + cloudIds.get(prevLocalId) + ". Version is only available for current user.");
                    saveProgress(providerId, processed.get(versionIds.get(prevLocalId)), false);
                }
            }
        }
        if (errors > 0)
            logger.warn("Migration of " + providerId + " encoutered " + errors + " errors.");
        return (counter - errors) == providerPaths.size();
    }

    private void saveProgress(String providerId, List<String> strings, boolean truncate) {
        try {
            Path dest = FileSystems.getDefault().getPath(".", providerId + ".txt");
            if (truncate) {
                // make copy
                Path bkp = FileSystems.getDefault().getPath(".", providerId + ".bkp");
                int c = 0;
                while (Files.exists(bkp))
                    bkp = FileSystems.getDefault().getPath(".", providerId + ".bkp" + String.valueOf(c++));

                Files.copy(dest, bkp);
                // truncate and write to empty file
                Files.write(dest, strings, Charset.forName("UTF-8"), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE);
            } else
                Files.write(dest, strings, Charset.forName("UTF-8"), StandardOpenOption.CREATE, StandardOpenOption.APPEND, StandardOpenOption.WRITE);
        } catch (IOException e) {
            logger.error("Progress file " + providerId + ".txt could not be saved.", e);
        }
    }

    private URI persistVersion(String cloudId, String version) {
        int retries = DEFAULT_RETRIES;
        while (retries-- > 0) {
            try {
                return mcs.persistRepresentation(cloudId, resourceProvider.getRepresentationName(), version);
            } catch (ProcessingException e) {
                logger.warn("Error processing HTTP request while persisting version: " + version + " for record: " + cloudId + ". Retries left: " + retries, e);
            } catch (MCSException e) {
                logger.error("ECloud error when persisting version: " + version + " for record: " + cloudId, e);
                if (e.getCause() instanceof ConnectException) {
                    logger.warn("Connection timeout error when persisting version: " + version + " for record: " + cloudId + ". Retries left: " + retries);
                } else if (e.getCause() instanceof SocketTimeoutException) {
                    logger.warn("Read time out error when persisting version: " + version + " for record: " + cloudId + ". Retries left: " + retries);
                } else
                    break;
            }
        }
        return null;
    }

    private boolean permitVersion(String cloudId, String version) {
        int retries = DEFAULT_RETRIES;
        while (retries-- > 0) {
            try {
                mcs.permitVersion(cloudId, resourceProvider.getRepresentationName(), version);
                return true;
            } catch (ProcessingException e) {
                logger.warn("Error processing HTTP request while granting permissions to version: " + version + " for record: " + cloudId + ". Retries left: " + retries, e);
            } catch (MCSException e) {
                logger.error("ECloud error when granting permissions to version: " + version + " for record: " + cloudId, e);
                if (e.getCause() instanceof ConnectException) {
                    logger.warn("Connection timeout error when granting permissions to version: " + version + " for record: " + cloudId + ". Retries left: " + retries);
                } else if (e.getCause() instanceof SocketTimeoutException) {
                    logger.warn("Read time out error when granting permissions to version: " + version + " for record: " + cloudId + ". Retries left: " + retries);
                } else
                    break;
            }
        }
        return false;
    }

    /*
    URI parameter is the version URI returned when creating representation name, it ends with a version identifier
     */
    private String getVersionIdentifier(URI uri) {
        String uriStr = uri.toString();
        int pos = uriStr.lastIndexOf("/");
        if (pos != -1)
            return uriStr.substring(pos + 1);
        return null;
    }

    private class ProviderMigrator implements Callable<MigrationResult> {
        String providerId;
        List<FilePaths> paths;

        ProviderMigrator(String providerId, List<FilePaths> paths) {
            this.providerId = providerId;
            this.paths = paths;
        }

        public MigrationResult call()
                throws Exception {
            long start = System.currentTimeMillis();
            boolean success = true;

            for (FilePaths fp : paths)
                success &= processProvider(providerId, fp);

            return new MigrationResult(success, providerId, (float) (System.currentTimeMillis() - start) / (float) 1000);
        }
    }

    private class MigrationResult {
        Boolean success;
        String providerId;

        float time;

        MigrationResult(Boolean success, String providerId, float time) {
            this.success = success;
            this.providerId = providerId;
            this.time = time;
        }

        String getProviderId() {
            return providerId;
        }

        Boolean isSuccessful() {
            return success;
        }

        public float getTime() {
            return time;
        }
    }

    public void clean(String providerId) {
        try {
            List<String> toSave = new ArrayList<String>();

            new Cleaner().cleanRecords(providerId, mcs, uis);

            for (String line : Files.readAllLines(FileSystems.getDefault().getPath(".", providerId + ".txt"), Charset.forName("UTF-8"))) {
                StringTokenizer st = new StringTokenizer(line, "=");
                if (st.hasMoreTokens()) {
                    st.nextToken();
                }
                String url = st.nextToken();
                int pos = url.indexOf("/records/");
                if (pos > -1) {
                    String id = url.substring(pos + "/records/".length());
                    id = id.substring(0, id.indexOf("/"));
                    int retries = DEFAULT_RETRIES;
                    while (retries-- > 0) {
                        try {
                            mcs.deleteRecord(id);
                            uis.deleteCloudId(id);
                            break;
                        } catch (ProcessingException e) {
                            logger.warn("Error processing HTTP request while deleting record: " + id + ". Retries left: " + retries, e);
                            if (retries == 0) // when this is the last unsuccessful try
                                toSave.add(line);
                        } catch (RecordNotExistsException e) {
                            // no record, no problem
                            break;
                        } catch (CloudException e) {
                            // nothing to do
                            logger.warn("Could not delete record.", e);
                            if (retries == 0) // when this is the last unsuccessful try
                                toSave.add(line);
                        } catch (MCSException e) {
                            logger.warn("Could not delete record.", e);
                            if (retries == 0) // when this is the last unsuccessful try
                                toSave.add(line);
                        } catch (Exception e) {
                            logger.warn("Could not delete record.", e);
                            if (retries == 0) // when this is the last unsuccessful try
                                toSave.add(line);
                        }
                    }
                }
            }
            saveProgress(providerId, toSave, true);
        } catch (IOException e) {
            logger.error("Problem with file.", e);
        }
    }
}
