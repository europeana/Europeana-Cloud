package eu.europeana.cloud.migrator;

import eu.europeana.cloud.common.model.DataProviderProperties;

import java.net.URI;
import java.util.List;
import java.util.Map;

public interface ResourceProvider {

    /**
     * Get data provider identifier as string.
     *
     * @param path path to resource
     * @return data provider identifier
     */
    public String getProviderId(String path);

    /**
     * Get basic information on data provider such as organisation name, address and others.
     * See <code>DataProviderProperties</code> for details.
     *
     * @param path path to resource
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
    public String getLocalIdentifier(String location, String path);

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
     * Determines user filename from the specified path to file.
     *
     * @param path    path to file either local or remote in URI syntax
     *
     * @return filename to be stored in ECloud
     */
    public String getFilename(String path);
}
