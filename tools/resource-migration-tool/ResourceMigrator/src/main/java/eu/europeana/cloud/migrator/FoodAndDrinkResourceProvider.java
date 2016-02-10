package eu.europeana.cloud.migrator;

import eu.europeana.cloud.common.model.DataProviderProperties;
import org.apache.log4j.Logger;

import java.io.File;

public class FoodAndDrinkResourceProvider
        extends DefaultResourceProvider {
    private static final String DATA_PROVIDERS_DIR = "users";

    private static final Logger logger = Logger.getLogger(FoodAndDrinkResourceProvider.class);


    public FoodAndDrinkResourceProvider(String representationName, String mappingFile, String locations) {
        super(representationName, mappingFile, locations);
    }

    /**
     * Return local record identifier from the path to image file. Path must contain provider identifier
     * and must point to the file.
     *
     * @param location not used here
     * @param path path to image file
     * @return local identifier
     */
    @Override
    public String getLocalIdentifier(String location, String path) {
        String providerId = getProviderId(path);
        if (path == null || path.isEmpty() || providerId == null || providerId.isEmpty()) {
            logger.warn("Either path or provider identifier is null or empty. Local record identifier cannot be retrieved.");
            return null;
        }

        int pos = path.indexOf(providerId);
        if (pos != -1) {
            String local = path.substring(pos + providerId.length());
            if (local.startsWith(ResourceMigrator.LINUX_SEPARATOR) || local.startsWith(ResourceMigrator.WINDOWS_SEPARATOR))
                return local.substring(1);
        }
        logger.warn("Path to the image file does not contain provider identifier. Local record identifier cannot be retrieved.");
        return null;
    }

    @Override
    public String getProviderId(String path) {
        if (!path.contains(DATA_PROVIDERS_DIR)) {
            logger.error("No data providers directory found in resource path");
            return null;
        }

        int pos = path.indexOf(DATA_PROVIDERS_DIR);
        String rest = path.substring(pos + DATA_PROVIDERS_DIR.length());
        if (rest.startsWith(ResourceMigrator.LINUX_SEPARATOR) || rest.startsWith(ResourceMigrator.WINDOWS_SEPARATOR))
            rest = rest.substring(1);
        pos = rest.indexOf(ResourceMigrator.LINUX_SEPARATOR);
        if (pos == -1)
            pos = rest.indexOf(ResourceMigrator.WINDOWS_SEPARATOR);

        return rest.substring(0, pos > -1 ? pos : rest.length());
    }

    @Override
    public DataProviderProperties getDataProviderProperties(String path) {
        File f = new File(path);
        // maybe this is path to image inside the data provider directory
        if (f.isFile())
            f = f.getParentFile();
        if (!f.isDirectory())
            return getDefaultDataProviderProperties();

        // assume we can find a file with data provider properties named id.properties
        String id = getProviderId(path);
        File dpFile = new File(f, id + PROPERTIES_EXTENSION);
        if (!dpFile.exists())
            return getDefaultDataProviderProperties();
        return getDataProviderPropertiesFromFile(dpFile);
    }

    @Override
    public void migrate() {

    }

    /**
     * User filename must contain provider id and all intermediate directory names between provider directory and image filename
     *
     * @param path    path to file either local or remote in URI syntax
     *
     * @return filename containing provider id and image filename
     */
    @Override
    public String getFilename(String path) {
        String providerId = getProviderId(path);
        int pos = path.indexOf(providerId);
        if (pos == -1)
            return null;
        return path.substring(pos);
    }
}
