package eu.europeana.cloud.migrator;

import eu.europeana.cloud.common.model.DataProviderProperties;

import java.net.URI;
import java.util.List;
import java.util.Map;

public interface ResourceProvider {

    /**
     * Get resource provider identifier as string. Resource provider is usually institution that has
     * its directory within the resource location. It is NOT the data provider used in ECloud however
     * the same identifier may be used.
     *
     * @param path path to resource
     * @return resource provider identifier
     */
    public String getResourceProviderId(String path);


    /**
     * Returns the data provider identifier used in ECloud. Usually it is the same for all ResourceProvider records
     * and it's defined in configuration file.
     *
     * @param path path to file that may be used to determine the data provider identifier if it's not defined in configuration
     * @return data provider identifier for ECloud
     */
    public String getDataProviderId(String path);

    /**
     * Get basic information on data provider such as organisation name, address and others.
     * See <code>DataProviderProperties</code> for details.
     *
     * @param path path to directory where the data provider properties file is located
     * @return data provider properties
     */
    public DataProviderProperties getDataProviderProperties(String path);

    /**
     * Get local identifier of the record for the specified path to file and provider identifier.
     *
     * @param location location of the file path
     * @param path path to file
     * @return local identifier of the record
     */
    public String getLocalIdentifier(String location, String path, boolean duplicate);

    /**
     * Return the default representation name of records added for this resource provider.
     * The representation name is the same for all records within one run of migration tool.
     *
     * @return default representation name
     */
    public String getRepresentationName();

    /**
     * Return resource provider location as URI. This may indicate directory or remote resource URL.
     *
     * @return resource provider locaction as URI
     */
    public List<URI> getLocations();

    /**
     * Checks whether the provider location is local.
     *
     * @return true when location is local or false otherwise
     */
    public boolean isLocal();

    /**
     * Scan location in case it's local. Otherwise return an empty map.
     * The returned map should associate provider identifier (it can be determined from the path) with the list of files to add.
     *
     * @return map of provider identifiers associated to the list of paths to files
     */
    public Map<String, List<FilePaths>> scan();

    /**
     * Determines user filename from the specified path to file and location.
     *
     * @param location location where the path is placed, usually needed to determine file path relative to location
     * @param path    path to file either local or remote in URI syntax
     *
     * @return filename to be stored in ECloud
     */
    public String getFilename(String location, String path);

    /**
     * Determine the number of files that should be added for the specified local identifier.
     * It may be done using the mapping file or any other way proper for the resource provider.
     * When it is impossible to determine -1 is returned.
     *
     * @param localId local identifier of the record
     * @return number of files the record should have or -1 if it's impossible to determine
     */
    public int getFileCount(String localId);
}
