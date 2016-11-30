package eu.europeana.cloud.migrator.provider;

import eu.europeana.cloud.common.model.DataProviderProperties;
import eu.europeana.cloud.migrator.ResourceMigrator;
import org.apache.log4j.Logger;

import java.io.File;

public class RoyalArmoryResourceProvider
        extends DefaultResourceProvider {

    private static final Logger logger = Logger.getLogger(RoyalArmoryResourceProvider.class);


    public RoyalArmoryResourceProvider(String representationName, String mappingFile, String locations, String dataProviderId) {
        super(representationName, mappingFile, locations, dataProviderId);
    }

    /**
     * Return local record identifier from the path to image file. Path must
     * point to the file and must start with location.
     *
     * @param location  location where the file resides
     * @param path      path to image file
     * @param duplicate not used here
     * @return local identifier
     */
    @Override
    public String getLocalIdentifier(String location, String path, boolean duplicate) {
        // royal armory cannot have duplicates, always return null in such case
        if (duplicate)
            return null;

        if (location == null || path == null)
            return null;

        String fileName = getFilename(location, path);
        if (fileName == null)
            logger.warn("Path to the image file does not start with location path. Local record identifier cannot be retrieved.");
        return fileName;
    }

    @Override
    public String getResourceProviderId(String path) {
        return getDataProviderId(path);
    }

    /**
     * RoyalArmoryResourceProvider always uses the specified data provider identifier.
     *
     * @param path path to file that is not used
     * @return data provider identifier for ECloud
     */
    @Override
    public String getDataProviderId(String path) {
        if (dataProviderId == null)
            throw new IllegalArgumentException("Data provider must be supplied!");
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
     * User filename must start with location
     *
     * @param path path to file
     * @return filename containing provider id and image filename
     */
    @Override
    public String getFilename(String location, String path) {
        int pos = path.indexOf(location);
        if (pos != -1) {
            String local = path.substring(pos + location.length());
            if (local.startsWith(ResourceMigrator.LINUX_SEPARATOR) || local.startsWith(ResourceMigrator.WINDOWS_SEPARATOR))
                return local.substring(1);
        }
        return null;
    }


    @Override
    public int getFileCount(String localId) {
        return 1;
    }
}
