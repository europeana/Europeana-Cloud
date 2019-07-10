package eu.europeana.cloud.migrator.provider;

import eu.europeana.cloud.common.model.DataProviderProperties;
import eu.europeana.cloud.migrator.ResourceMigrator;

import java.io.File;

public class StandardResourceProvider
        extends DefaultResourceProvider {


    private static final String DEFAULT_PROVIDER_ID = "data_provider";

    public StandardResourceProvider(String representationName, String mappingFile, String locations, String dataProviderId) {
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

        if (!path.startsWith(location))
            return null;

        int pos = path.indexOf(getDataProviderId(path));
        if (pos == -1)
            return null;

        String local = path.substring(pos + getDataProviderId(path).length());
        if (local.startsWith(ResourceMigrator.LINUX_SEPARATOR) || local.startsWith(ResourceMigrator.WINDOWS_SEPARATOR))
            local = local.substring(1);
        pos = local.indexOf(ResourceMigrator.LINUX_SEPARATOR);
        if (pos == -1)
            pos = local.indexOf(ResourceMigrator.WINDOWS_SEPARATOR);
        if (pos == -1)
            return null;

        return local.substring(0, pos);
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
        if (dataProviderId != null)
            return dataProviderId;
        return DEFAULT_PROVIDER_ID;
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
     * @return filename
     */
    @Override
    public String getFilename(String location, String path) {
        if (path == null || location == null)
            return null;

        if (!path.startsWith(location))
            return null;

        int pos = path.lastIndexOf(ResourceMigrator.LINUX_SEPARATOR);
        if (pos == -1)
            pos = path.lastIndexOf(ResourceMigrator.WINDOWS_SEPARATOR);
        return path.substring(pos + 1);
    }


    @Override
    public int getFileCount(String localId) {
        return 1;
    }
}
