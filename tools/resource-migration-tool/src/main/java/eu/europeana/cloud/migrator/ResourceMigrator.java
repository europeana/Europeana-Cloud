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
import java.io.*;
import java.net.*;
import java.nio.charset.Charset;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;

public class ResourceMigrator {

    public static final String LINUX_SEPARATOR = "/";

    public static final String WINDOWS_SEPARATOR = "\\";

    public static final String TEXT_EXTENSION = ".txt";

    private static final String FILES_PART = "files";

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

    // Default threads pool size
    private static final byte DEFAULT_PROVIDER_POOL_SIZE = 50;

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

    /**
     * Key is directory name, value is identifier that should be used in ECloud
     */
    protected Map<String, String> dataProvidersMapping;

    protected int threadsCount = DEFAULT_PROVIDER_POOL_SIZE;

    public ResourceMigrator(ResourceProvider resProvider, String dataProvidersMappingFile, String threadsCount) throws IOException {
        this.resourceProvider = resProvider;
        this.dataProvidersMapping = readDataProvidersMapping(dataProvidersMappingFile);
        if (threadsCount != null && !threadsCount.isEmpty()) {
            try {
                this.threadsCount = Integer.valueOf(threadsCount);
                if (this.threadsCount < 0)
                    this.threadsCount = DEFAULT_PROVIDER_POOL_SIZE;
            } catch (NumberFormatException e) {
                // leave the default value
            }
        }
    }

    private Map<String, String> readDataProvidersMapping(String dataProvidersMappingFile) throws IOException {
        Map<String, String> mapping = new HashMap<String, String>();
        // data providers mapping is optional so return empty map when not provided
        if (dataProvidersMappingFile == null || dataProvidersMappingFile.isEmpty())
            return mapping;

        Path mappingPath = null;
        try {
            // try to treat the mapping file as local file
            mappingPath = FileSystems.getDefault().getPath(".", dataProvidersMappingFile);
            if (!mappingPath.toFile().exists())
                mappingPath = FileSystems.getDefault().getPath(dataProvidersMappingFile);
        } catch (InvalidPathException e) {
            // in case path cannot be created try to treat the mapping file as absolute path
            mappingPath = FileSystems.getDefault().getPath(dataProvidersMappingFile);
        }
        if (mappingPath == null || !mappingPath.toFile().exists())
            throw new IOException("Mapping file cannot be found: " + dataProvidersMappingFile);

        String directory;
        String identifier;

        for (String line : Files.readAllLines(mappingPath, Charset.forName("UTF-8"))) {
            if (line == null)
                break;
            StringTokenizer tokenizer = new StringTokenizer(line, ";");
            // first token is directory name
            if (tokenizer.hasMoreTokens())
                directory = tokenizer.nextToken().trim();
            else
                directory = null;
            if (directory == null || directory.isEmpty()) {
                logger.warn("Directory name for data provider is missing (" + directory + "). Skipping line.");
                continue;
            }

            if (tokenizer.hasMoreTokens())
                identifier = tokenizer.nextToken().trim();
            else
                identifier = null;

            // when identifier is null or empty do not add to map
            if (identifier == null || identifier.isEmpty())
                continue;
            mapping.put(directory, identifier);
        }

        return mapping;
    }

    public boolean migrate(boolean clean, boolean simulate) {
        boolean success = true;

        long start = System.currentTimeMillis();

        // key is provider id, value is a list of files to add
        Map<String, List<FilePaths>> paths = resourceProvider.scan();

//        if (logger.isDebugEnabled()) {
//            for (Map.Entry<String, List<FilePaths>> entry : paths.entrySet()) {
//                for (FilePaths fp : entry.getValue()) {
//                    logger.debug("Found paths for provider " + entry.getKey() + " in location " + fp.getLocation() + " (" + fp.getFullPaths().size() + "):");
//                    for (String s : fp.getFullPaths())
//                        logger.debug(s);
//                }
//            }
//        }

        logger.info("Scanning resource provider locations finished in " + String.valueOf(((float) (System.currentTimeMillis() - start) / (float) 1000)) + " sec.");

        // when simulate is true just a simulation is performed - it scans the locations and stores summary to a file
        if (!clean && simulate) {
            summarize(paths);
            return true;
        }

        List<Future<MigrationResult>> results = null;
        List<Callable<MigrationResult>> tasks = new ArrayList<Callable<MigrationResult>>();

        // create task for each resource provider
        for (String providerId : paths.keySet()) {
            if (clean) {
                logger.info("Cleaning " + providerId);
                clean(providerId);
            }
            if (!simulate) {
                logger.info("Starting task thread for provider " + providerId + "...");
                tasks.add(new ProviderMigrator(providerId, paths.get(providerId), null));
            }
        }

        // when simulation mode is on no tasks to invoke should be created, return immediately
        if (tasks.size() == 0)
            return success;

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

    private void summarize(Map<String, List<FilePaths>> paths) {
        String summaryFile = "simulation" + System.currentTimeMillis() + ResourceMigrator.TEXT_EXTENSION;
        try {
            // create a file called simulation{timestamp}.txt

            Path dest = FileSystems.getDefault().getPath(".", summaryFile);
            String msg = "Found " + paths.size() + " data providers.\n";
            Files.write(dest, msg.getBytes(Charset.forName("UTF-8")), StandardOpenOption.CREATE, StandardOpenOption.APPEND, StandardOpenOption.WRITE);
            for (Map.Entry<String, List<FilePaths>> entry : paths.entrySet()) {
                msg = "\nData provider " + entry.getKey();
                if (dataProvidersMapping.get(entry.getKey()) != null)
                    msg += " (mapped: " + dataProvidersMapping.get(entry.getKey()) + ")";
                msg += ": " + entry.getValue().size() + " locations\n";
                Files.write(dest, msg.getBytes(Charset.forName("UTF-8")), StandardOpenOption.CREATE, StandardOpenOption.APPEND, StandardOpenOption.WRITE);
                for (FilePaths fp : entry.getValue()) {
                    BufferedReader reader = fp.getPathsReader();
                    try {
                        msg = "\nLocation: " + fp.getLocation() + " - " + fp.size() + " paths\n\n";
                        Files.write(dest, msg.getBytes(Charset.forName("UTF-8")), StandardOpenOption.CREATE, StandardOpenOption.APPEND, StandardOpenOption.WRITE);
                        int counter = 1;
                        if (reader != null) {
                            for (; ; ) {
                                String s = reader.readLine();
                                if (s == null)
                                    break;
                                Files.write(dest, String.valueOf(counter++ + ". " + s + "\n").getBytes(Charset.forName("UTF-8")), StandardOpenOption.CREATE, StandardOpenOption.APPEND, StandardOpenOption.WRITE);
                            }
                        }
                        else {
                            for (String s : fp.getFullPaths()) {
                                Files.write(dest, String.valueOf(counter++ + ". " + s + "\n").getBytes(Charset.forName("UTF-8")), StandardOpenOption.CREATE, StandardOpenOption.APPEND, StandardOpenOption.WRITE);
                            }
                        }
                    } finally {
                        if (reader != null)
                            reader.close();
                    }
                }
            }
        } catch (IOException e) {
            logger.error("Summary file " + summaryFile + " could not be saved.", e);
        }
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
        // get the mapped identifier if any
        String dataProviderId = getProviderId(providerId);
        while (retries-- > 0) {
            try {
                // get all representations
                List<Representation> representations;

                try {
                    representations = mcs.getRepresentations(cloudId, resourceProvider.getRepresentationName());
                } catch (RepresentationNotExistsException e) {
                    // when there are no representations add a new one
                    return mcs.createRepresentation(cloudId, resourceProvider.getRepresentationName(), dataProviderId);
                }

                // when there are some old representations it means that somebody had to add them, delete non persistent ones
                for (Representation representation : representations) {
                    if (representation.isPersistent())
                        return null;
                    else
                        mcs.deleteRepresentation(cloudId, resourceProvider.getRepresentationName(), representation.getVersion());
                }
                // when there are no representations add a new one
                return mcs.createRepresentation(cloudId, resourceProvider.getRepresentationName(), dataProviderId);
            } catch (ProcessingException e) {
                logger.warn("Error processing HTTP request while creating representation for record " + cloudId + ", provider " + dataProviderId + ". Retries left: " + retries, e);
            } catch (ProviderNotExistsException e) {
                logger.error("Provider " + dataProviderId + " does not exist!");
                break;
            } catch (RecordNotExistsException e) {
                logger.error("Record " + cloudId + " does not exist!");
                break;
            } catch (MCSException e) {
                logger.error("Problem with creating representation name!");
            } catch (Exception e) {
                logger.error("Exception when creating representation occured.", e);
            }
        }
        logger.warn("All attempts to create representation failed. ProviderId: " + providerId + " CloudId: " + cloudId + " Representation: " + resourceProvider.getRepresentationName());
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
        // get mapped data provider identifier if any
        String dataProviderId = getProviderId(providerId);
        while (retries-- > 0) {
            try {
                CloudId cloudId = uis.createCloudId(dataProviderId, localId);
                if (cloudId != null)
                    return cloudId.getId();
            } catch (ProcessingException e) {
                logger.warn("Error processing HTTP request while creating record for provider " + dataProviderId + " and local id " + localId + ". Retries left: " + retries, e);
            } catch (CloudException e) {
                if (e.getCause() instanceof RecordExistsException) {
                    try {
                        logger.info("Record Exists" + localId);
                        return uis.getCloudId(dataProviderId, localId).getId();
                    } catch (ProcessingException e1) {
                        logger.warn("Error processing HTTP request while creating record for provider " + dataProviderId + ". Retries left: " + retries, e);
                    } catch (CloudException e1) {
                        logger.warn("Record for provider " + dataProviderId + " with local id " + localId + " could not be created", e1);
                        break;
                    } catch (Exception e1) {
                        logger.info("Provider: " + providerId + " Local: " + localId, e);
                    }
                } else {
                    logger.warn("Record for provider " + dataProviderId + " with local id " + localId + " could not be created", e);
                    break;
                }
            } catch (Exception e) {
                logger.error("Exception when creating record occured.", e);
            }
        }
        logger.warn("All attempts to create record failed. ProviderId: " + providerId + " LocalId: " + localId);
        return null;
    }

    /**
     * Creates file name in ECloud.
     *
     * @param path     path to file, if location is remote it must have correct URI syntax, otherwise it must be a proper path in a filesystem
     * @param version  version string, must be appropriate for the given record
     * @param recordId global record identifier
     */
    private String createFilename(String location, String path, String version, String recordId) {
        // when any of input parameter is null it is impossible to prepare filename
        if (location == null || path == null || version == null || recordId == null)
            return null;

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

                String fileName = resourceProvider.getFilename(location, resourceProvider.isLocal() ? path : fullURI.toString());
                if (logger.isDebugEnabled())
                    logger.debug("Trying to upload file with name: " + fileName);

                // path should contain proper slash characters ("/")
                URI result = fsc.uploadFile(recordId, resourceProvider.getRepresentationName(), version, fileName, is, mimeType);
                // operations below replace the filename got from File Service Client to the original filename, because the FSC returns encoded values of / and diacritic characters
                if (result != null) {
                    String fileURL = result.toString();
                    int i = fileURL.indexOf(FILES_PART);
                    // when there is no "files" part of the URL return the originally retrieved one
                    if (i == -1)
                        return fileURL;
                    // remove the filename part
                    fileURL = fileURL.substring(0, i + FILES_PART.length() + 1);
                    // add original filename
                    fileURL += fileName;
                    return fileURL;
                }
            } catch (ProcessingException e) {
                logger.warn("Processing HTTP request failed. Upload file: " + resourceProvider.getFilename(location, fullURI.toString()) + " for record id: " + recordId + ". Retries left: " + retries);
            } catch (SocketTimeoutException e) {
                logger.warn("Read time out. Upload file: " + resourceProvider.getFilename(location, fullURI.toString()) + " for record id: " + recordId + ". Retries left: " + retries);
            } catch (ConnectException e) {
                logger.error("Connection timeout. Upload file: " + resourceProvider.getFilename(location, fullURI.toString()) + " for record id: " + recordId + ". Retries left: " + retries);
            } catch (FileNotFoundException e) {
                logger.error("Could not open input stream to file!", e);
            } catch (IOException e) {
                logger.error("Problem with detecting mime type from file (" + path + ")", e);
            } catch (MCSException e) {
                logger.error("ECloud error when uploading file.", e);
            } catch (Exception e) {
                logger.error("ECloud error when uploading file.", e);
            } finally {
                try {
                    is.close();
                } catch (IOException e) {
                    logger.warn("Could not close stream.", e);
                }
            }
        }
        logger.warn("All attempts to upload file failed. Location: " + location + " Path: " + path + " Version: " + version + " RecordId: " + recordId);
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

    private String getProviderId(String providerId) {
        String mapped = dataProvidersMapping.get(providerId);
        if (mapped == null)
            return providerId;
        return mapped;
    }

    synchronized private String createProvider(String path) {
        // get mapped data provider identifier
        String providerId = getProviderId(resourceProvider.getDataProviderId(path));
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
            } catch (Exception e) {
                logger.error("Exception when creating provider occured.", e);
            }
        }
        logger.warn("All attempts to create data provider failed. Provider: " + providerId + " Path: " + path);
        return null;
    }

    private void removeProcessedPaths(String providerId, FilePaths paths) {
        try {
            List<String> processed = new ArrayList<String>();
            for (String line : Files.readAllLines(FileSystems.getDefault().getPath(".", providerId + ResourceMigrator.TEXT_EXTENSION), Charset.forName("UTF-8"))) {
                StringTokenizer st = new StringTokenizer(line, ";");
                if (st.hasMoreTokens()) {
                    processed.add(st.nextToken());
                }
            }
            paths.removeAll(processed);
        } catch (IOException e) {
            logger.warn("Progress file for provider " + providerId + " could not be opened. Returning all paths.");
        }
    }


    private boolean processProvider(String resourceProviderId, FilePaths providerPaths) {
        // first remove already processed paths, if there is no progress file for the provider no filtering is performed
        removeProcessedPaths(providerPaths.getIdentifier() != null ? providerPaths.getIdentifier() : resourceProviderId, providerPaths);

        boolean result = true;

        try {
            if (providerPaths.size() > 0) {
                String dataProviderId = retrieveDataProviderId(providerPaths);
                if (dataProviderId == null)
                {
                    logger.error("Cannot determine data provider.");
                    return false;
                }

                // first create provider, pass the path to the possible properties file, use first path to determine data provider id
                String propsFile = providerPaths.getLocation() + LINUX_SEPARATOR + dataProviderId + DefaultResourceProvider.PROPERTIES_EXTENSION;
                if (createProvider(propsFile) == null) {
                    // when create provider was not successful finish processing
                    return false;
                }

                List<String> duplicates = new ArrayList<String>();
                result &= processPaths(providerPaths, false, duplicates);
                if (duplicates.size() > 0) {
                    FilePaths duplicatePaths = new FilePaths(providerPaths.getLocation(), providerPaths.getDataProvider());
                    duplicatePaths.setIdentifier(providerPaths.getIdentifier());
                    duplicatePaths.getFullPaths().addAll(duplicates);
                    result &= processPaths(duplicatePaths, true, null);
                }
            }
        } catch (Exception e) {
            // when any uncaught exception occurs just catch it and report false result
            return false;
        }
        return result;
    }

    private String retrieveDataProviderId(FilePaths providerPaths) {
        String path = null;

        BufferedReader reader = providerPaths.getPathsReader();
        if (reader != null) {
            try {
                path = reader.readLine();
            } catch (IOException e) {
                e.printStackTrace();
            }
            finally {
                try {
                    reader.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        else
            path = providerPaths.size() > 0 ? providerPaths.getFullPaths().get(0) : null;
        if (path == null)
            return null;
        return resourceProvider.getDataProviderId(path);
    }

    private boolean processPaths(FilePaths fp, boolean duplicate, List<String> duplicates) throws IOException {
        if (fp == null)
            return false;

        // Resource provider identifier
        String resourceProviderId = fp.getDataProvider();

        // Location of the files
        String location = fp.getLocation();

        // Identifier of the file paths list
        String identifier = fp.getIdentifier();

        // key is local identifier, value is cloud identifier
        Map<String, String> cloudIds = new HashMap<String, String>();
        // key is local identifier, value is version identifier
        Map<String, String> versionIds = new HashMap<String, String>();
        // key is version identifier, value is a list of strings containing path=URI
        Map<String, List<String>> processed = new HashMap<String, List<String>>();
        // key is local identifier, value is files that were added
        Map<String, Integer> fileCount = new HashMap<String, Integer>();

        int counter = 0;
        int errors = 0;

        int size = fp.size();

        if (size == 0)
            return false;

        BufferedReader reader = fp.getPathsReader();

        String prevLocalId = null;
        try {
            String path;
            for (; ; ) {
                if ((int) (((float) (counter) / (float) size) * 100) > (int) (((float) (counter - 1) / (float) size) * 100))
                    logger.info("Resource provider: " + resourceProviderId + "." + (identifier.equals(resourceProviderId) ? "" : (" Part: " + identifier + ".")) + " Progress: " + counter + " of " + size + " (" + (int) (((float) (counter) / (float) size) * 100) + "%). Errors: " + errors + ". Duplicates: " + duplicate);
                if (reader == null) {
                    // paths in list
                    if (counter >= size)
                        break;
                    path = fp.getFullPaths().get(counter);
                }
                else {
                    path = reader.readLine();
                    if (path == null)
                        break;
                }
                counter++;
                // get local record identifier
                String localId = resourceProvider.getLocalIdentifier(location, path, duplicate);
                if (localId == null) {
                    if (!duplicate) {
                        // check whether there is a duplicate record for the path and store the path for later use if so
                        if (resourceProvider.getLocalIdentifier(location, path, true) != null)
                            duplicates.add(path);
                    }
                    // when local identifier is null it means that the path may be wrong
                    logger.warn("Local identifier for path: " + path + " could not be obtained. Skipping path...");
                    continue;
                }

                if (!duplicate) {
                    // check whether there is a duplicate record for the path and store the path for later use if so
                    if (resourceProvider.getLocalIdentifier(location, path, true) != null)
                        duplicates.add(path);
                }

                if (logger.isDebugEnabled()) {
                    logger.debug("Local identifier for path: " + path + ": " + localId);
                }
                if (!localId.equals(prevLocalId)) {
                    if (prevLocalId != null && cloudIds.get(prevLocalId) != null && versionIds.get(prevLocalId) != null && resourceProvider.getFileCount(prevLocalId) == fileCount.get(prevLocalId)) {
                        if (logger.isDebugEnabled())
                            logger.debug("Record " + prevLocalId + " complete. Saving...");
                        // persist previous version if it was created
                        URI persistent = persistVersion(cloudIds.get(prevLocalId), versionIds.get(prevLocalId));
                        if (persistent != null) {
                            if (!permitVersion(cloudIds.get(prevLocalId), versionIds.get(prevLocalId)))
                                logger.warn("Could not grant permissions to version " + versionIds.get(prevLocalId) + " of record " + cloudIds.get(prevLocalId) + ". Version is only available for current user.");
                            saveProgress(fp.getIdentifier() != null ? fp.getIdentifier() : resourceProviderId, processed.get(versionIds.get(prevLocalId)), false);
                            // remove already saved paths
                            processed.get(versionIds.get(prevLocalId)).clear();
                            processed.remove(versionIds.get(prevLocalId));
                            cloudIds.remove(prevLocalId);
                            versionIds.remove(prevLocalId);
                        }
                    }
                    prevLocalId = localId;
                }
                if (cloudIds.get(localId) == null) {
                    // create new record when it was not created before
                    String cloudId = createRecord(resourceProvider.getDataProviderId(path), localId);
                    if (logger.isDebugEnabled()) {
                        logger.debug("Cloud identifier for path: " + path + ": " + cloudId);
                    }
                    if (cloudId == null) {
                        // this is an error
                        errors++;
                        continue; // skip this path
                    }
                    cloudIds.put(localId, cloudId);
                    // create representation for the record
                    URI uri = createRepresentationName(resourceProvider.getDataProviderId(path), cloudId);
                    if (logger.isDebugEnabled()) {
                        logger.debug("Representation for path: " + path + ": " + (uri != null ? uri.toString() : "null"));
                    }
                    if (uri == null) {
                        // this is not an error, version is already there and is persistent
                        continue; // skip this path
                    }
                    String verId = getVersionIdentifier(uri);
                    if (logger.isDebugEnabled()) {
                        logger.debug("Version identifier for path: " + path + ": " + verId);
                    }
                    if (verId == null) {
                        // this is an error, version identifier could not be retrieved from representation URI
                        errors++;
                        continue; // skip this path
                    }
                    versionIds.put(localId, verId);
                }

                if (logger.isDebugEnabled()) {
                    logger.debug("Before creating file: \nLocation: " + location + "\nPath: " + path + "\nVersion: " + versionIds.get(localId) + "\nCloudId: " + cloudIds.get(localId));
                }

                // create file name in ECloud with the specified name
                String fileAdded = createFilename(location, path, versionIds.get(localId), cloudIds.get(localId));
                if (fileAdded == null) {
                    // this is an error, upload failed
                    prevLocalId = null; // this should prevent persisting the version and saving progress
                    continue; // skip this path
                }
                // put the created URI for path to processed list
                if (processed.get(versionIds.get(localId)) == null)
                    processed.put(versionIds.get(localId), new ArrayList<String>());
                processed.get(versionIds.get(localId)).add(path + ";" + fileAdded);
                // increase file count or set to 1 if it's a first file
                fileCount.put(localId, fileCount.get(localId) != null ? (fileCount.get(localId) + 1) : 1);
            }
            if (prevLocalId != null && resourceProvider.getFileCount(prevLocalId) == fileCount.get(prevLocalId)) {
                if (logger.isDebugEnabled())
                    logger.debug("Record " + prevLocalId + " complete. Saving...");
                // persist previous version
                URI persistent = persistVersion(cloudIds.get(prevLocalId), versionIds.get(prevLocalId));
                if (persistent != null) {
                    if (!permitVersion(cloudIds.get(prevLocalId), versionIds.get(prevLocalId)))
                        logger.warn("Could not grant permissions to version " + versionIds.get(prevLocalId) + " of record " + cloudIds.get(prevLocalId) + ". Version is only available for current user.");
                    saveProgress(fp.getIdentifier() != null ? fp.getIdentifier() : resourceProviderId, processed.get(versionIds.get(prevLocalId)), false);
                    processed.get(versionIds.get(prevLocalId)).clear();
                    processed.remove(versionIds.get(prevLocalId));
                    cloudIds.remove(prevLocalId);
                    versionIds.remove(prevLocalId);
                }
            }
        }
        finally {
            if (reader != null)
                reader.close();
        }
        if (errors > 0)
            logger.warn("Migration of " + resourceProviderId + " encoutered " + errors + " errors.");
        return (counter - errors) == size;
    }

    private void saveProgress(String providerId, List<String> strings, boolean truncate) {
        try {
            Path dest = FileSystems.getDefault().getPath(".", providerId + ResourceMigrator.TEXT_EXTENSION);
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
        // resource provider identifier
        private String providerId;

        // paths to files that will be migrated
        private List<FilePaths> paths;

        // identifier of part of file paths (usually a name of the directory), may be null
        private String identifier;

        // Pool of threads used to migrate files
        private final ExecutorService threadProviderPool = Executors
                .newFixedThreadPool(threadsCount);

        ProviderMigrator(String providerId, List<FilePaths> paths, String identifier) {
            this.providerId = providerId;
            this.paths = paths;
            this.identifier = identifier;
        }

        public MigrationResult call()
                throws Exception {
            long start = System.currentTimeMillis();
            boolean success = true;

            List<FilePaths> split = resourceProvider.split(paths);
            if (split.equals(paths)) {
                // when split operation did not change anything just run the migration for the given paths
                for (FilePaths fp : paths)
                    success &= processProvider(providerId, fp);
            } else { // initial paths were split into more sets, for each set run separate thread and gather results
                List<Future<MigrationResult>> results = null;
                List<Callable<MigrationResult>> tasks = new ArrayList<Callable<MigrationResult>>();

                boolean mergeProgress = false;

                // create task for each file path
                for (FilePaths fp : split) {
                    logger.info("Starting task thread for file paths " + fp.getIdentifier() + "...");
                    List<FilePaths> lst = new ArrayList<FilePaths>();
                    lst.add(fp);
                    mergeProgress |= !fp.getIdentifier().equals(fp.getDataProvider());
                    tasks.add(new ProviderMigrator(providerId, lst, fp.getIdentifier()));
                }

                try {
                    // invoke a separate thread for each provider
                    results = threadProviderPool.invokeAll(tasks);

                    MigrationResult partResult;
                    for (Future<MigrationResult> result : results) {
                        partResult = result.get();
                        logger.info("Migration of part " + partResult.getIdentifier() + " (" + partResult.getProviderId() + ") performed " + (partResult.isSuccessful() ? "" : "un") + "successfully. Migration time: " + partResult.getTime() + " sec.");
                        success &= partResult.isSuccessful();
                    }
                    // concatenate progress files to one if necessary
                    if (mergeProgress)
                        saveProgressFromThreads(providerId, split);
                } catch (InterruptedException e) {
                    logger.error("Migration processed interrupted.", e);
                } catch (ExecutionException e) {
                    logger.error("Problem with migration task thread execution.", e);
                }

            }

            return new MigrationResult(success, providerId, (float) (System.currentTimeMillis() - start) / (float) 1000, identifier);
        }
    }

    private void saveProgressFromThreads(String providerId, List<FilePaths> paths) {
        BufferedReader reader = null;

        try {
            // new progress file with provider name
            Path dest = FileSystems.getDefault().getPath(".", providerId + ResourceMigrator.TEXT_EXTENSION);
            if (Files.exists(dest)) {
                // make copy
                Path bkp = FileSystems.getDefault().getPath(".", providerId + ".bkp");
                int c = 0;
                while (Files.exists(bkp))
                    bkp = FileSystems.getDefault().getPath(".", providerId + ".bkp" + String.valueOf(c++));

                Files.copy(dest, bkp);
                Files.delete(dest);
            }

            // read every file for a paths object and append it to the destination file
            for (FilePaths fp : paths) {
                if (fp.getIdentifier() == null || fp.getIdentifier().equals(providerId))
                    continue;
                Path progressFile = FileSystems.getDefault().getPath(".", fp.getIdentifier() + ResourceMigrator.TEXT_EXTENSION);
                try {
                    reader = Files.newBufferedReader(progressFile, Charset.forName("UTF-8"));
                    for (; ; ) {
                        String line = reader.readLine();
                        if (line == null)
                            break;
                        if (!line.endsWith("\n"))
                            line += "\n";
                        Files.write(dest, line.getBytes(Charset.forName("UTF-8")), StandardOpenOption.CREATE, StandardOpenOption.APPEND, StandardOpenOption.WRITE);
                    }
                } catch (IOException e) {
                    // do nothing, move to the next file
                    logger.warn("Problem with file " + progressFile.toAbsolutePath().toString());
                } finally {
                    if (reader != null)
                        reader.close();
                }
            }
        } catch (IOException e) {
            logger.error("Progress file " + providerId + ".txt could not be saved.", e);
        }
    }

    private class MigrationResult {
        // success indicator
        Boolean success;

        // resource provider identifier
        String providerId;

        // Part identifier
        String identifier;

        // execution time
        float time;

        MigrationResult(Boolean success, String providerId, float time, String identifier) {
            this.success = success;
            this.providerId = providerId;
            this.time = time;
            this.identifier = identifier;
        }

        String getProviderId() {
            return providerId;
        }

        Boolean isSuccessful() {
            return success;
        }

        float getTime() {
            return time;
        }

        String getIdentifier() {
            return identifier;
        }
    }

    public void clean(String providerId) {
        try {
            List<String> toSave = new ArrayList<String>();

            new Cleaner().cleanRecords(providerId, mcs, uis);

            for (String line : Files.readAllLines(FileSystems.getDefault().getPath(".", providerId + ResourceMigrator.TEXT_EXTENSION), Charset.forName("UTF-8"))) {
                StringTokenizer st = new StringTokenizer(line, ";");
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
