package eu.europeana.cloud.migrator;

import eu.europeana.cloud.common.model.DataProviderProperties;
import org.apache.log4j.Logger;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class FoodAndDrinkResourceProvider
        extends DefaultResourceProvider {
    private static final String DATA_PROVIDERS_DIR = "users";

    private static final Logger logger = Logger.getLogger(FoodAndDrinkResourceProvider.class);

    public FoodAndDrinkResourceProvider(String representationName, String mappingFile, String locations, String dataProviderId) {
        super(representationName, mappingFile, locations, dataProviderId);
    }

    /**
     * Return local record identifier from the path to image file. Path must contain provider identifier
     * and must point to the file.
     *
     * @param location  not used here
     * @param path      path to image file
     * @param duplicate not used here
     * @return local identifier
     */
    @Override
    public String getLocalIdentifier(String location, String path, boolean duplicate) {
        // food and drink cannot have duplicates, always return null in such case
        if (duplicate)
            return null;

        String providerId = getResourceProviderId(path);
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
    public String getResourceProviderId(String path) {
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

    /**
     * FoodAndDrinkResourceProvider uses the resource provider as the data provider identifier.
     *
     * @param path path to file that may be used to determine the data provider identifier if it's not defined in configuration
     * @return data provider identifier for ECloud
     */
    @Override
    public String getDataProviderId(String path) {
        if (dataProviderId == null)
            return getResourceProviderId(path);
        return dataProviderId;
    }

    /**
     * Input path must point to the file that is either the properties file or the directory that contains
     * properties file named id.properties where id is identifier of data provider used in ECloud
     *
     * @param path path to directory where the data provider properties file is located
     * @return
     */
    @Override
    public DataProviderProperties getDataProviderProperties(String path) {
        String id = getDataProviderId(path);

        // data provider identifier cannot be null, when it is - it means that the path is wrong
        if (id == null)
            return getDefaultDataProviderProperties();

        // create file object from the path
        File f = new File(path);
        // when file is id.properties return properties from file
        if (f.exists() && f.isFile() && f.getName().equals(id + PROPERTIES_EXTENSION))
            return getDataProviderPropertiesFromFile(f);

        // when file is directory try to search for file id.properties inside
        if (f.isDirectory()) {
            File dpFile = new File(f, id + PROPERTIES_EXTENSION);
            if (dpFile.exists())
                return getDataProviderPropertiesFromFile(dpFile);
        }

        return getDefaultDataProviderProperties();
    }


    /**
     * User filename must contain provider id and all intermediate directory names between provider directory and image filename
     *
     * @param path path to file either local or remote in URI syntax
     * @return filename containing provider id and image filename
     */
    @Override
    public String getFilename(String location, String path) {
        String providerId = getResourceProviderId(path);
        int pos = path.indexOf(providerId);
        if (pos == -1)
            return null;
        return path.substring(pos);
    }


    @Override
    public int getFileCount(String localId) {
        return 1;
    }
}
