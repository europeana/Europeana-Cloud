package eu.europeana.cloud.service.commons.urls;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Parser for URL used in eCloud API.<br/>
 * <br/><br/>
 * Some of possible URL patterns are:<br/>
 * <p/>
 * http://www.example.com/data-providers/DATAPROVIDER/data-sets/
 * http://www.example.com/data-providers/DATAPROVIDER/data-sets/DATASET/
 * http://www.example.com/data-providers/DATAPROVIDER/data-sets/DATASET/assignments/
 * http://www.example.com/records/CLOUDID/
 * http://www.example.com/records/CLOUDID/representations/
 * http://www.example.com/records/CLOUDID/representations/REPRESENTATIONNAME/
 * http://www.example.com/records/CLOUDID/representations/REPRESENTATIONNAME/versions/
 * http://www.example.com/records/CLOUDID/representations/REPRESENTATIONNAME/versions/VERSION/
 * http://www.example.com/records/CLOUDID/representations/REPRESENTATIONNAME/versions/VERSION/copy/
 * http://www.example.com/records/CLOUDID/representations/REPRESENTATIONNAME/versions/VERSION/permit/
 * http://www.example.com/records/CLOUDID/representations/REPRESENTATIONNAME/versions/VERSION/persist/
 * http://www.example.com/records/CLOUDID/representations/REPRESENTATIONNAME/versions/VERSION/users/username/permit/permission/
 * http://www.example.com/records/CLOUDID/representations/REPRESENTATIONNAME/versions/VERSION/files/
 * http://www.example.com/records/CLOUDID/representations/REPRESENTATIONNAME/versions/VERSION/files/FILENAME/
 */
public class UrlParser {
    private static final char DIR_SEPARATOR = '/';
    private static final List<UrlPart> CLOUD_ID_URL_PATTERN = Arrays.asList(UrlPart.CONTEXT, UrlPart.RECORDS);
    private static final List<UrlPart> REPRESENTATIONS_URL_PATTERN = Arrays.asList(UrlPart.CONTEXT, UrlPart.RECORDS, UrlPart.REPRESENTATIONS);
    private static final List<UrlPart> REPRESENTATION_VERSION_URL_PATTERN = Arrays.asList(UrlPart.CONTEXT, UrlPart.RECORDS, UrlPart.REPRESENTATIONS, UrlPart.VERSIONS);
    private static final List<UrlPart> REPRESENTATION_VERSION_FILE_URL_PATTERN = Arrays.asList(UrlPart.CONTEXT, UrlPart.RECORDS, UrlPart.REPRESENTATIONS, UrlPart.VERSIONS, UrlPart.FILES);
    private static final List<UrlPart> DATASET_URL_PATTERN = Arrays.asList(UrlPart.CONTEXT, UrlPart.DATA_PROVIDERS, UrlPart.DATA_SETS);

    private String[] values;
    private Map<UrlPart, String> parts = new LinkedHashMap<>();

    private URL resourceUrl;
    private String appLocation;
    private UrlBuilder ecloudUrlBuilder;

    public UrlParser(String url) throws MalformedURLException {
        resourceUrl = new URL(url);
        appLocation = resourceUrl.getProtocol() + "://" + resourceUrl.getHost();
        if (resourceUrl.getPort() != -1) {
            appLocation += ":" + resourceUrl.getPort();
        }
        String path = resourceUrl.getPath();
        if (path.startsWith("/")) {
            path = path.substring(1);
        }
        values = path.split("/");
        for (int i = 0; i < values.length; i++) {
            if (values[i].equalsIgnoreCase(UrlPart.RECORDS.getValue())) {
                if (values.length > i + 1) {
                    parts.put(UrlPart.RECORDS, values[i + 1]);
                    i++;
                } else {
                    parts.put(UrlPart.RECORDS, null);
                }
            } else if (values[i].equalsIgnoreCase(UrlPart.REPRESENTATIONS.getValue())) {
                if (values.length > i + 1) {
                    parts.put(UrlPart.REPRESENTATIONS, values[i + 1]);
                    i++;
                } else {
                    parts.put(UrlPart.REPRESENTATIONS, null);
                }
            } else if (values[i].equalsIgnoreCase(UrlPart.VERSIONS.getValue())) {
                if (values.length > i + 1) {
                    parts.put(UrlPart.VERSIONS, values[i + 1]);
                    i++;
                } else {
                    parts.put(UrlPart.VERSIONS, null);
                }
            } else if (values[i].equalsIgnoreCase(UrlPart.FILES.getValue())) {
                if (values.length > i + 1) {
                    final StringBuilder filePath = new StringBuilder();
                    do {
                        filePath.append(DIR_SEPARATOR + values[i + 1]);
                    } while (++i < values.length - 1);
                    parts.put(UrlPart.FILES, filePath.toString());
                } else {
                    parts.put(UrlPart.FILES, null);
                }
            } else if (values[i].equalsIgnoreCase(UrlPart.DATA_SETS.getValue())) {
                if (values.length > i + 1) {
                    parts.put(UrlPart.DATA_SETS, values[i + 1]);
                    i++;
                } else {
                    parts.put(UrlPart.DATA_SETS, null);
                }
            } else if (values[i].equalsIgnoreCase(UrlPart.DATA_PROVIDERS.getValue())) {
                if (values.length > i + 1) {
                    parts.put(UrlPart.DATA_PROVIDERS, values[i + 1]);
                    i++;
                } else {
                    parts.put(UrlPart.DATA_PROVIDERS, null);
                }
            } else {
                if (i == 0) {
                    parts.put(UrlPart.CONTEXT, values[i]);
                }
            }
        }

        ecloudUrlBuilder = new UrlBuilder(parts);
    }

    /**
     * Checks if given URL points to data-sets list<br/>
     * <p/>
     * E.g. http://www.example.com/data-providers/DATAPROVIDER/data-sets/
     *
     * @return
     */
    public boolean isUrlToDatasetsList() {
        return matches(parts, DATASET_URL_PATTERN) && containEmptyValue(UrlPart.DATA_SETS);
    }

    /**
     * Checks if given URL points to data-set<br/>
     * <p/>
     * E.g. http://www.example.com/data-providers/DATAPROVIDER/data-sets/DATASET/
     *
     * @return
     */
    public boolean isUrlToDataset() {
        return matches(parts, DATASET_URL_PATTERN) && containNonEmptyValue(UrlPart.DATA_SETS);
    }

    /**
     * Checks if given URL points to representations list <br/>
     * E.g. http://www.example.com/records/CLOUDID/
     *
     * @return
     */
    public boolean isUrlToCloudId() {
        return matches(parts, CLOUD_ID_URL_PATTERN) && containNonEmptyValue(UrlPart.RECORDS);
    }

    /**
     * Checks if given URL points to representations list <br/>
     * E.g. http://www.example.com/records/CLOUDID/representations/
     *
     * @return
     */
    public boolean isUrlToRepresentations() {
        return matches(parts, REPRESENTATIONS_URL_PATTERN) && containEmptyValue(UrlPart.REPRESENTATIONS);
    }

    /**
     * Checks if given URL points to representation <br/>
     * E.g. http://www.example.com/records/CLOUDID/representations/REPRESENTATIONNAME/
     *
     * @return
     */
    public boolean isUrlToRepresentation() {
        return matches(parts, REPRESENTATIONS_URL_PATTERN) && containNonEmptyValue(UrlPart.REPRESENTATIONS);
    }

    /**
     * Checks if given URL points to representation versions list <br/>
     * E.g. http://www.example.com/records/CLOUDID/representations/REPRESENTATIONNAME/versions
     *
     * @return
     */
    public boolean isUrlToRepresentationVersions() {
        return matches(parts, REPRESENTATION_VERSION_URL_PATTERN) && containEmptyValue(UrlPart.VERSIONS);
    }

    /**
     * Checks if given URL points to representation version<br/>
     * E.g. http://www.example.com/records/CLOUDID/representations/REPRESENTATIONNAME/versions/VERSION
     *
     * @return
     */
    public boolean isUrlToRepresentationVersion() {
        return matches(parts, REPRESENTATION_VERSION_URL_PATTERN) && containNonEmptyValue(UrlPart.VERSIONS);
    }

    /**
     * Checks if given URL points to files list.<br/>
     * E.g. http://127.0.0.1:8080/mcs/records/FUWQ4WMUGIGEHVA3X7FY5PA3DR5Q4B2C4TWKNILLS6EM4SJNTVEQ/representations/TIFF/versions/86318b00-6377-11e5-a1c6-90e6ba2d09ef/files
     *
     * @return
     */
    public boolean isUrlToRepresentationVersionFiles() {
        return containEmptyValue(UrlPart.FILES);
    }

    /**
     * Checks if given URL points to file.<br/>
     * E.g. http://127.0.0.1:8080/mcs/records/FUWQ4WMUGIGEHVA3X7FY5PA3DR5Q4B2C4TWKNILLS6EM4SJNTVEQ/representations/TIFF/versions/86318b00-6377-11e5-a1c6-90e6ba2d09ef/files/sampleFileName.txt
     *
     * @return
     */
    public boolean isUrlToRepresentationVersionFile() {
        return matches(parts, REPRESENTATION_VERSION_FILE_URL_PATTERN) && containNonEmptyValue(UrlPart.FILES);
    }

    private boolean containEmptyValue(UrlPart urlPart) {
        return parts.containsKey(urlPart) && (parts.get(urlPart) == null || parts.get(urlPart).isEmpty());
    }

    private boolean containNonEmptyValue(UrlPart urlPart) {
        return parts.containsKey(urlPart) && parts.get(urlPart) != null && !parts.get(urlPart).isEmpty();
    }

    private boolean matches(Map<UrlPart, String> parts, List<UrlPart> pattern) {
        int counter = 0;

        if (parts.keySet().size() != pattern.size()) {
            return false;
        }
        for (Map.Entry<UrlPart, String> entry : parts.entrySet()) {
            UrlPart urlPart = entry.getKey();
            UrlPart part = pattern.get(counter);
            if (part != null && urlPart.equals(part)) {
                counter++;
                continue;
            }
            return false;
        }
        return true;
    }

    /**
     * Returns one part of given URL. If requested part does not exist, null will be returned
     *
     * @param urlPart which one part should be returned.
     * @return
     */
    public String getPart(UrlPart urlPart) {
        return parts.get(urlPart);
    }

    /**
     * Returns URL to version based on provided URL. If it is not possible UrlBuilderException will be thrown
     *
     * @return
     */
    public String getVersionUrl() throws UrlBuilderException {
        UrlBuilder ecloudUrlBuilder = new UrlBuilder(parts);
        String result = ecloudUrlBuilder
                .clear()
                .withCloudID()
                .withRepresentation()
                .withVersion()
                .build();

        return appLocation + result;
    }

    public String getVersionsUrl() throws UrlBuilderException {
        UrlBuilder ecloudUrlBuilder = new UrlBuilder(parts);
        String result = ecloudUrlBuilder
                .clear()
                .withCloudID()
                .withRepresentation()
                .withVersionWithoutValue()
                .build();

        return appLocation + result;
    }

    public String getDataSetsUrl() throws UrlBuilderException {
        ecloudUrlBuilder = new UrlBuilder(parts);
        String result = ecloudUrlBuilder
                .clear()
                .withDataProvider()
                .withDataSetWithoutValue().build();

        return appLocation + result;
    }
}