package eu.europeana.cloud.service.mcs.utils;

import com.google.common.collect.ImmutableMap;
import eu.europeana.cloud.common.model.DataSet;
import eu.europeana.cloud.common.model.File;
import eu.europeana.cloud.common.model.Record;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.service.mcs.rest.DataSetResource;
import eu.europeana.cloud.service.mcs.rest.FileResource;
import eu.europeana.cloud.service.mcs.rest.RepresentationVersionResource;
import eu.europeana.cloud.service.mcs.rest.RepresentationVersionsResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.UriInfo;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Properties;

import static eu.europeana.cloud.common.web.ParamConstants.*;

/**
 * Utility class that inserts absolute uris into classes that will be used as REST responses.
 */
public final class EnrichUriUtil {
    private static final Logger LOGGER = LoggerFactory.getLogger(EnrichUriUtil.class);

    private  static  final  MappingPlaceholdersResolver PROPERTY_PLACEHOLDER_HELPER
            = new MappingPlaceholdersResolver();

    private EnrichUriUtil() {
    }

    @Deprecated
    public static void enrich(UriInfo uriInfo, Record record) {
        for (Representation rep : record.getRepresentations()) {
            enrich(uriInfo, rep);
        }
    }

    public static void enrich(HttpServletRequest httpServletRequest, Record record) {
        for (Representation rep : record.getRepresentations()) {
            enrich(httpServletRequest, rep);
        }
    }


    @Deprecated
    public static void enrich(UriInfo uriInfo, Representation representation) {
        URI allVersionsUri = uriInfo
                .getBaseUriBuilder()
                .path(RepresentationVersionsResource.class)
                .buildFromMap(
                    ImmutableMap.of(CLOUD_ID, representation.getCloudId(), REPRESENTATION_NAME,
                        representation.getRepresentationName()));
        representation.setAllVersionsUri(uriInfo.resolve(allVersionsUri));

        if (representation.getVersion() != null) {
            URI latestVersionUri = uriInfo
                    .getBaseUriBuilder()
                    .path(RepresentationVersionResource.class)
                    .buildFromMap(
                        ImmutableMap.of(CLOUD_ID, representation.getCloudId(), REPRESENTATION_NAME,
                            representation.getRepresentationName(), VERSION, representation.getVersion()));
            representation.setUri(uriInfo.resolve(latestVersionUri));
        }
        if (representation.getFiles() != null) {
            for (File f : representation.getFiles()) {
                enrich(uriInfo, representation, f);
            }
        }
    }

    public static void enrich(HttpServletRequest httpServletRequest, Representation representation) {
        Properties properties = new Properties();
        properties.setProperty(CLOUD_ID, representation.getCloudId());
        properties.setProperty(REPRESENTATION_NAME, representation.getRepresentationName());

        representation.setAllVersionsUri(createURI(httpServletRequest, RepresentationVersionsResource.CLASS_MAPPING, properties));

        if(representation.getVersion() != null) {
            properties.setProperty(VERSION, representation.getVersion());
            representation.setUri(createURI(httpServletRequest, RepresentationVersionResource.CLASS_MAPPING, properties));
        }

        if (representation.getFiles() != null) {
            for (File f : representation.getFiles()) {
                enrich(httpServletRequest, representation, f);
            }
        }

    }

    @Deprecated
    public static void enrich(UriInfo uriInfo, Representation rep, File file) {
        enrich(uriInfo, rep.getCloudId(), rep.getRepresentationName(), rep.getVersion(), file);
    }

    public static void enrich(HttpServletRequest httpServletRequest, Representation rep, File file) {
        enrich(httpServletRequest, rep.getCloudId(), rep.getRepresentationName(), rep.getVersion(), file);
    }

    @Deprecated
    public static void enrich(UriInfo uriInfo, String cloudId, String representationName, String version, File file) {
        URI fileUri = uriInfo
                .getBaseUriBuilder()
                .path(FileResource.class)
                .buildFromMap(
                    ImmutableMap.of(CLOUD_ID, cloudId, REPRESENTATION_NAME, representationName, VERSION, version, FILE_NAME,
                        file.getFileName()));
        file.setContentUri(uriInfo.resolve(fileUri));
    }

    public static void enrich(HttpServletRequest httpServletRequest, String cloudId, String representationName, String version, File file) {
        Properties properties = new Properties();
        properties.setProperty(CLOUD_ID, cloudId);
        properties.setProperty(REPRESENTATION_NAME, representationName);
        properties.setProperty(VERSION, version);
        properties.setProperty(FILE_NAME, file.getFileName());

        file.setContentUri(createURI(httpServletRequest, FileResource.CLASS_MAPPING, properties));
    }

    @Deprecated
    public static void enrich(UriInfo uriInfo, DataSet dataSet) {
        URI datasetUri = uriInfo.getBaseUriBuilder().path(DataSetResource.class)
                .buildFromMap(ImmutableMap.of(PROVIDER_ID, dataSet.getProviderId(), DATA_SET_ID, dataSet.getId()));
        dataSet.setUri(uriInfo.resolve(datasetUri));
    }

    public static void enrich(HttpServletRequest httpServletRequest, DataSet dataSet) {
        Properties properties = new Properties();

        properties.setProperty(PROVIDER_ID, dataSet.getProviderId());
        properties.setProperty(DATA_SET_ID, dataSet.getId());

        dataSet.setUri(createURI(httpServletRequest, DataSetResource.CLASS_MAPPING, properties));
    }

    private static URI createURI(HttpServletRequest httpServletRequest, String path, Properties properties) {
        URI result = null;

        StringBuilder uriSpec = new StringBuilder();
        uriSpec.append(httpServletRequest.getContextPath());
        uriSpec.append(PROPERTY_PLACEHOLDER_HELPER.replacePlaceholders(path, properties));
        try {
            URL url = new URL(
                    httpServletRequest.getScheme(),
                    httpServletRequest.getServerName(),
                    httpServletRequest.getServerPort(),
                    uriSpec.toString());

            result= url.toURI();
        } catch (MalformedURLException | URISyntaxException ure) {
            LOGGER.warn("Invalid URL/URI", ure);
        }

        return result;
    }

    public static class MappingPlaceholdersResolver {
        private static final char OPEN = '{';
        private static final char CLOSE = '}';
        private static final char REG_EXP_SEPRATOR = ':';


        public String replacePlaceholders(String path, Properties properties) {
            StringBuilder result = new StringBuilder();

            int currentIndex = 0;
            int prevIndex = 0;

            while(currentIndex != -1) {
                currentIndex = path.indexOf(OPEN, prevIndex);
                if(currentIndex != -1) {
                    result.append(path.substring(prevIndex, currentIndex));

                    int closeIndex = path.indexOf(CLOSE, currentIndex);
                    if(closeIndex == -1) {
                        result.append(path.substring(prevIndex));
                    } else {
                        prevIndex = processPlaceholder(path.substring(currentIndex, closeIndex+1), properties, closeIndex, result);
                    }
                } else {
                    result.append(path.substring(prevIndex));
                }
            }
            return result.toString();
        }

        private int processPlaceholder(String placeHolder, Properties properties, int closeIndex, StringBuilder result) {
            int regExpSepLocalIndex = placeHolder.indexOf(REG_EXP_SEPRATOR);

            String placeholderName;
            String placeholderRegExp = null;

            if(regExpSepLocalIndex == -1) {
                placeholderName = placeHolder.substring(1, placeHolder.length()-1).trim();
            } else {
                placeholderName = placeHolder.substring(1, regExpSepLocalIndex).trim();
                placeholderRegExp = placeHolder.substring(regExpSepLocalIndex+1, placeHolder.length()-1).trim();
            }

            if(!properties.containsKey(placeholderName)) {
                result.append(placeHolder);
            } else {
                String value = properties.getProperty(placeholderName);

                if(placeholderRegExp == null || value.matches(placeholderRegExp)) {
                    result.append(value);
                } else {
                    result.append(placeHolder);
                }
            }

            return closeIndex+1;
        }

    }
}
