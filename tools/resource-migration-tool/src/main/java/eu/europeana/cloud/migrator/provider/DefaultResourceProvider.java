package eu.europeana.cloud.migrator.provider;

import eu.europeana.cloud.common.model.DataProviderProperties;
import eu.europeana.cloud.migrator.ResourceMigrator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

public abstract class DefaultResourceProvider
        implements ResourceProvider {

    // Keys for data provider properties
    public static final String ORGANISATION_NAME_KEY = "organisationName";

    public static final String OFFICIAL_ADDRESS_KEY = "officialAddress";

    public static final String ORGANISATION_WEBSITE_KEY = "organisationWebsite";

    public static final String ORGANISATION_WEBSITE_URL_KEY = "organisationWebsiteURL";

    public static final String DIGITAL_LIBRARY_WEBSITE_KEY = "digitalLibraryWebsite";

    public static final String DIGITAL_LIBRARY_WEBSITE_URL_KEY = "digitalLibraryURL";

    public static final String CONTACT_PERSON_KEY = "contactPerson";

    public static final String REMARKS_KEY = "remarks";

    // Default data provider properties
    public static final String DEFAULT_ORGANISATION_NAME = "Example Organisation";

    public static final String DEFAULT_OFFICIAL_ADDRESS = "Example Address";

    public static final String DEFAULT_ORGANISATION_WEBSITE = "Example Website";

    public static final String DEFAULT_ORGANISATION_WEBSITE_URL = "http://www.example.com";

    public static final String DEFAULT_DIGITAL_LIBRARY_WEBSITE = "Example DL Website";

    public static final String DEFAULT_DIGITAL_LIBRARY_WEBSITE_URL = "http://www.example.com/digital";

    public static final String DEFAULT_CONTACT_PERSON = "John Example";

    public static final String DEFAULT_REMARKS = "Example remarks";

    public static final String PROPERTIES_EXTENSION = ".properties";

    public static final int DEFAULT_LIST_SIZE = 500;

    // Logger
    private static final Logger logger = LoggerFactory.getLogger(DefaultResourceProvider.class);

    // Representation name from configuration
    protected String representationName;

    // Mapping file storing the association between local identifiers and files/directories in the filesystem
    protected String mappingFile;

    // list of location retrieved from configuration file
    protected List<URI> locations;

    // local indicator = when false the resource location is remote
    protected boolean local;

    // map associating resource provider with its location
    private Map<String, URI> providersLocation;

    // data provider identifier, may be null, if null the identifier is derived from the file path, otherwise it is used for all files
    protected String dataProviderId;

    protected DefaultResourceProvider(String representationName, String mappingFile, String locations, String dataProviderId) {
        if (representationName == null)
            throw new IllegalArgumentException("Representation name cannot be null!");
        this.representationName = representationName;
        this.mappingFile = mappingFile;
        this.locations = new ArrayList<URI>();
        this.providersLocation = new HashMap<String, URI>();
        this.local = detectLocations(locations);
        this.dataProviderId = dataProviderId != null ? (dataProviderId.isEmpty() ? null : dataProviderId) : null;
    }

    private boolean detectLocations(String locations) {
        boolean allLocal = true;
        boolean firstChange = true;
        String[] locs = locations.split(";");
        for (String loc : locs) {
            URI location;
            try {
                location = new URI(loc);
                this.locations.add(location);
            } catch (URISyntaxException e) {
                logger.error("URI " + loc + " is not valid.", e);
                continue;
            }
            String scheme = location.getScheme();
            boolean locationLocal = (scheme == null || scheme.isEmpty() || scheme.toLowerCase().equals("file"));
            if (locationLocal != allLocal) {
                if (firstChange) {
                    firstChange = false;
                    allLocal = locationLocal;
                } else {
                    logger.error("All locations must be either local or remote");
                    throw new IllegalArgumentException("All locations must be either local or remote. locations: " + locations);
                }
            }
        }
        return allLocal;
    }

    protected DataProviderProperties getDataProviderPropertiesFromFile(File dpFile) {
        Properties props = loadPropertiesFile(dpFile);
        return new DataProviderProperties(props.getProperty(ORGANISATION_NAME_KEY, DEFAULT_ORGANISATION_NAME),
                props.getProperty(OFFICIAL_ADDRESS_KEY, DEFAULT_OFFICIAL_ADDRESS),
                props.getProperty(ORGANISATION_WEBSITE_KEY, DEFAULT_ORGANISATION_WEBSITE),
                props.getProperty(ORGANISATION_WEBSITE_URL_KEY, DEFAULT_ORGANISATION_WEBSITE_URL),
                props.getProperty(DIGITAL_LIBRARY_WEBSITE_KEY, DEFAULT_DIGITAL_LIBRARY_WEBSITE),
                props.getProperty(DIGITAL_LIBRARY_WEBSITE_URL_KEY, DEFAULT_DIGITAL_LIBRARY_WEBSITE_URL),
                props.getProperty(CONTACT_PERSON_KEY, DEFAULT_CONTACT_PERSON),
                props.getProperty(REMARKS_KEY, DEFAULT_REMARKS));
    }

    private Properties loadPropertiesFile(File dpFile) {
        Properties props = new Properties();
        try(InputStream is= new FileInputStream(dpFile)) {
            props.load(is);
        } catch (IOException e) {
            logger.error("Problem with file " + dpFile.getAbsolutePath(), e);
        }
        return props;
    }

    protected DataProviderProperties getDefaultDataProviderProperties() {
        return new DataProviderProperties(DEFAULT_ORGANISATION_NAME,
                DEFAULT_OFFICIAL_ADDRESS,
                DEFAULT_ORGANISATION_WEBSITE,
                DEFAULT_ORGANISATION_WEBSITE_URL,
                DEFAULT_DIGITAL_LIBRARY_WEBSITE,
                DEFAULT_DIGITAL_LIBRARY_WEBSITE_URL,
                DEFAULT_CONTACT_PERSON,
                DEFAULT_REMARKS);
    }

    @Override
    public String getRepresentationName() {
        return representationName;
    }


    @Override
    public List<URI> getLocations() {
        return locations;
    }


    @Override
    public boolean isLocal() {
        return local;
    }

    @Override
    public Map<String, List<FilePaths>> scan() {
        Map<String, List<FilePaths>> paths = new HashMap<String, List<FilePaths>>();
        if (!local) {
            logger.warn("Location is not local. Scanning is not possible.");
            return paths;
        }

        for (URI location : locations) {
            collectPaths(Paths.get(location), paths, location);
            sortPaths(paths);
        }
        return paths;
    }

    private void sortPaths(Map<String, List<FilePaths>> paths) {
        for (List<FilePaths> list : paths.values()) {
            for (FilePaths fp : list)
                fp.sort();
        }
    }

    private void collectPaths(Path rootPath, Map<String, List<FilePaths>> paths, URI location) {
        if (rootPath == null)
            return;

        DirectoryStream<Path> dirStream = null;

        try {
            dirStream = Files.newDirectoryStream(rootPath);

            for (Iterator<Path> i = dirStream.iterator(); i.hasNext(); ) {
                Path path = i.next();
                if (Files.isDirectory(path))
                    collectPaths(path, paths, location);
                else {
                    String absolute = path.toAbsolutePath().toString().replace(ResourceMigrator.WINDOWS_SEPARATOR, ResourceMigrator.LINUX_SEPARATOR);
                    String providerId = getResourceProviderId(absolute);
                    if (absolute.endsWith(providerId + PROPERTIES_EXTENSION))
                        continue;
                    FilePaths providerPaths = getProviderPaths(Paths.get(location).toAbsolutePath().toString().replace(ResourceMigrator.WINDOWS_SEPARATOR, ResourceMigrator.LINUX_SEPARATOR), providerId, paths);
                    //providerPaths.getFullPaths().add(absolute);
                    providerPaths.addPath(absolute);
                    if (providersLocation.get(providerId) == null)
                        providersLocation.put(providerId, location);
                }
            }
        } catch (IOException e) {
            logger.error("There was a problem with opening direcory " + rootPath.toString(), e);
        } finally {
            if (dirStream != null)
                try {
                    dirStream.close();
                } catch (IOException e) {
                    logger.error("There was a problem with closing direcory stream for " + rootPath.toString(), e);
                }
        }
    }

    protected FilePaths getProviderPaths(String location, String providerId, Map<String, List<FilePaths>> paths) {
        if (paths.get(providerId) == null)
            paths.put(providerId, new ArrayList<FilePaths>());
        for (FilePaths p : paths.get(providerId)) {
            if (p.getLocation().equals(location))
                return p;
        }
        FilePaths fp = new FilePaths(location, providerId);
        if (usePathsFile())
            fp.useFile(location.replace(ResourceMigrator.LINUX_SEPARATOR, "_").replace(":", "_") + "_" + providerId);
        paths.get(providerId).add(fp);
        return fp;
    }

    public URI getProvidersLocation(String providerId) {
        return providersLocation.get(providerId);
    }

    /**
     * Default implementation of determining filename from path.
     * The result is either path without location, part of the specified path after "/" or "\" character or path itself.
     *
     * @param location location where path is placed, usually path starts with location
     * @param path     path to file either local or remote in URI syntax
     * @return filename to be stored in ECloud
     */
    @Override
    public String getFilename(String location, String path) {
        if (logger.isDebugEnabled()) {
            logger.debug("Get filename for location: {}, path: {}", location, path);
        }
        int pos = -1;
        if (path.startsWith(location)) {
            path = path.substring(location.length());
            if (logger.isDebugEnabled()) {
                logger.debug("Path without location: {}", path);
            }
            pos = path.indexOf(ResourceMigrator.LINUX_SEPARATOR);
            if (pos == -1)
                pos = path.indexOf(ResourceMigrator.WINDOWS_SEPARATOR);
        } else {
            pos = path.lastIndexOf(ResourceMigrator.LINUX_SEPARATOR);
            if (pos == -1)
                pos = path.lastIndexOf(ResourceMigrator.WINDOWS_SEPARATOR);
        }
        if (logger.isDebugEnabled()) {
            logger.debug("Returning: {}", path.substring(pos + 1));
        }
        // when pos == -1 whole path is returned, otherwise only part after pos
        return path.substring(pos + 1);
    }

    @Override
    public int getFileCount(String localId) {
        // impossible to determine in anstract class
        return -1;
    }


    /**
     * Default resource provider splits the paths list to several lists of more or less equal size around 500.
     *
     * @param paths
     * @return
     */
    @Override
    public List<FilePaths> split(List<FilePaths> paths) {
        List<FilePaths> result = new ArrayList<FilePaths>();
        for (FilePaths fp : paths) {
            result.addAll(split(fp));
        }
        return result;
    }

    private List<FilePaths> split(FilePaths fp) {
        // size of the list
        int size = fp.size();
        // number of full DEFAULT_LIST_SIZE parts
        int count = size / DEFAULT_LIST_SIZE;
        // size of the last part
        int rest = size % DEFAULT_LIST_SIZE;

        List<FilePaths> result = new ArrayList<FilePaths>();

        // when no need to split return the same file paths object
        if (size <= DEFAULT_LIST_SIZE)
            result.add(fp);
        else {
            for (int i = 0; i < count; i++) {
                FilePaths filePath = new FilePaths(fp.getLocation(), fp.getDataProvider());
                filePath.getFullPaths().addAll(fp.getFullPaths().subList(i * DEFAULT_LIST_SIZE, (i + 1) * DEFAULT_LIST_SIZE < size ? (i + 1) * DEFAULT_LIST_SIZE : size));
                filePath.setIdentifier(String.valueOf(i * DEFAULT_LIST_SIZE) + "_" + String.valueOf((i + 1) * DEFAULT_LIST_SIZE < size ? (i + 1) * DEFAULT_LIST_SIZE : size));
                result.add(filePath);
            }
            if (rest > 0) {
                FilePaths filePath = new FilePaths(fp.getLocation(), fp.getDataProvider());
                filePath.getFullPaths().addAll(fp.getFullPaths().subList(count * DEFAULT_LIST_SIZE, size));
                filePath.setIdentifier(String.valueOf(count * DEFAULT_LIST_SIZE) + "_" + String.valueOf(size));
                result.add(filePath);
            }
        }
        return result;
    }


    @Override
    public boolean usePathsFile() {
        return false;
    }


    @Override
    public Map<String, String> getReversedMapping() {
        return new HashMap<String, String>();
    }
}