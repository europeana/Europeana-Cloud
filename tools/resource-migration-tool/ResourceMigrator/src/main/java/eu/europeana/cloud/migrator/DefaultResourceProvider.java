package eu.europeana.cloud.migrator;

import eu.europeana.cloud.common.model.DataProviderProperties;
import org.apache.log4j.Logger;

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

    public static final String ORGANISATION_NAME_KEY = "organisationName";

    public static final String OFFICIAL_ADDRESS_KEY = "officialAddress";

    public static final String ORGANISATION_WEBSITE_KEY = "organisationWebsite";

    public static final String ORGANISATION_WEBSITE_URL_KEY = "organisationWebsiteURL";

    public static final String DIGITAL_LIBRARY_WEBSITE_KEY = "digitalLibraryWebsite";

    public static final String DIGITAL_LIBRARY_WEBSITE_URL_KEY = "digitalLibraryURL";

    public static final String CONTACT_PERSON_KEY = "contactPerson";

    public static final String REMARKS_KEY = "remarks";

    public static final String DEFAULT_ORGANISATION_NAME = "Example Organisation";

    public static final String DEFAULT_OFFICIAL_ADDRESS = "Example Address";

    public static final String DEFAULT_ORGANISATION_WEBSITE = "Example Website";

    public static final String DEFAULT_ORGANISATION_WEBSITE_URL = "http://www.example.com";

    public static final String DEFAULT_DIGITAL_LIBRARY_WEBSITE = "Example DL Website";

    public static final String DEFAULT_DIGITAL_LIBRARY_WEBSITE_URL = "http://www.example.com/digital";

    public static final String DEFAULT_CONTACT_PERSON = "John Example";

    public static final String DEFAULT_REMARKS = "Example remarks";

    private static final Logger logger = Logger.getLogger(DefaultResourceProvider.class);

    protected String representationName;

    protected String mappingFile;

    protected List<URI> locations;

    protected boolean local;

    private Map<String, URI> providersLocation;

    protected DefaultResourceProvider(String representationName, String mappingFile, String locations) {
        this.representationName = representationName;
        this.mappingFile = mappingFile;
        this.locations = new ArrayList<URI>();
        this.providersLocation = new HashMap<String, URI>();
        this.local = detectLocations(locations);
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
                } else
                    logger.error("All locations must be either local or remote");
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
        InputStream is = null;
        try {
            is = new FileInputStream(dpFile);
            props.load(is);
        } catch (IOException e) {
            logger.error("Problem with file " + dpFile.getAbsolutePath(), e);
        } finally {
            try {
                is.close();
            } catch (IOException e) {
                logger.error("Could not close input stream.", e);
            }
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
    public Map<String, List<String>> scan() {
        Map<String, List<String>> paths = new HashMap<String, List<String>>();
        if (!local) {
            logger.warn("Location is not local. Scanning is not possible.");
            return paths;
        }

        for (URI location : locations) {
            collectPaths(Paths.get(location), paths, location);
        }
        return paths;
    }

    private void collectPaths(Path rootPath, Map<String, List<String>> paths, URI location) {
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
                    String providerId = getProviderId(absolute);
                    if (paths.get(providerId) == null) {
                        List<String> list = new ArrayList<String>();
                        paths.put(providerId, list);
                    }
                    paths.get(providerId).add(absolute);
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

    public URI getProvidersLocation(String providerId) {
        return providersLocation.get(providerId);
    }

    /**
     * Default implementation of determining filename from path.
     * The result is either path without location, part of the specified path after "/" or "\" character or path itself.
     * @param path    path to file either local or remote in URI syntax
     *
     * @return filename to be stored in ECloud
     */
    @Override
    public String getFilename(String path) {
        URI location = providersLocation.get(getProviderId(path));
        if (path.startsWith(location.toString()))
            return path.substring(location.toString().length());
        else {
            int pos = path.lastIndexOf(ResourceMigrator.LINUX_SEPARATOR);
            if (pos == -1)
                pos = path.lastIndexOf(ResourceMigrator.WINDOWS_SEPARATOR);
            // when pos == -1 whole path is returned, otherwise only part after pos
            return path.substring(pos + 1);
        }
    }
}