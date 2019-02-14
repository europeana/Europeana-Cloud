package eu.europeana.cloud.service.dps.storm.topologies.media.service;

import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

import static eu.europeana.cloud.service.dps.PluginParameterKeys.RESOURCE_LINKS_COUNT;
import static eu.europeana.cloud.service.dps.PluginParameterKeys.RESOURCE_URL;

/**
 * Created by pwozniak on 2/5/19
 */
public class LinkCheckBolt extends AbstractDpsBolt {

    private static final Logger LOGGER = LoggerFactory.getLogger(LinkCheckBolt.class);

    private static final int CACHE_SIZE = 1024;
    Map<String, FileInfo> cache = new HashMap<>(CACHE_SIZE);

    private LinkChecker linkChecker;

    public LinkCheckBolt(LinkChecker linkChecker) {
        this.linkChecker = linkChecker;
    }

    @Override
    public void prepare() {
    }

    /**
     * Performs link checking for given tuple
     *
     * @param tuple tuple that will be used for link checking
     */
    @Override
    public void execute(StormTaskTuple tuple) {
        ResourceInfo resourceInfo = readResourceInfoFromTuple(tuple);
        if (!hasLinksForCheck(resourceInfo)) {
            prepareTuple(tuple);
            emit(tuple);
        } else {
            FileInfo edmFile = checkProvidedLink(resourceInfo);
            if (isFileFullyProcessed(edmFile)) {
                removeFileFromCache(edmFile);
                prepareTuple(tuple, edmFile);
                emit(tuple);
            }
        }
    }

    private ResourceInfo readResourceInfoFromTuple(StormTaskTuple tuple) {
        ResourceInfo resourceInfo = new ResourceInfo();
        resourceInfo.counter = Integer.parseInt(tuple.getParameter(RESOURCE_LINKS_COUNT));
        resourceInfo.edmUrl = tuple.getFileUrl();
        resourceInfo.linkUrl = tuple.getParameter(RESOURCE_URL);
        return resourceInfo;
    }

    private boolean hasLinksForCheck(ResourceInfo resourceInfo) {
        return resourceInfo.counter > 0;
    }

    private FileInfo checkProvidedLink(ResourceInfo resourceInfo) {
        FileInfo edmFile = takeFileFromCache(resourceInfo);
        if (edmFile == null) {
            edmFile = new FileInfo(resourceInfo.edmUrl, resourceInfo.counter, 0);
            checkLink(resourceInfo, edmFile);
            putFileToCache(edmFile);
        } else {
            checkLink(resourceInfo, edmFile);
        }
        return edmFile;
    }

    private boolean isFileFullyProcessed(FileInfo fileInfo) {
        return fileInfo.linksChecked >= fileInfo.expectedNumberOfLinks;
    }

    private FileInfo takeFileFromCache(ResourceInfo resourceInfo) {
        return cache.get(resourceInfo.edmUrl);
    }

    private void putFileToCache(FileInfo fileInfo) {
        cache.put(fileInfo.fileUrl, fileInfo);
    }

    private void removeFileFromCache(FileInfo fileInfo) {
        cache.remove(fileInfo.fileUrl);
    }

    private void checkLink(ResourceInfo resourceInfo, FileInfo fileInfo) {
        LOGGER.info("Checking resource url {}", resourceInfo.edmUrl);
        try {
            int status = linkChecker.check(resourceInfo.linkUrl);
            if (!isResponseStatusAcceptable(status)) {
                LOGGER.info("Link error (code {}) for {}", status, resourceInfo.linkUrl);
                addErrorMessage(resourceInfo, fileInfo, status);
            }
        } catch (Exception e) {
            LOGGER.info("There was exception while checking the link: {}", resourceInfo.edmUrl);
            fileInfo.errors = fileInfo.errors + "," + e.getMessage();
        } finally {
            fileInfo.linksChecked++;
        }
    }

    private void addErrorMessage(ResourceInfo resourceInfo, FileInfo fileInfo, int status) {
        if (!fileInfo.errors.isEmpty())
            fileInfo.errors = fileInfo.errors + ", ";
        fileInfo.errors = fileInfo.errors + "Link error (code " + status + ") for " + resourceInfo.linkUrl;
    }

    private boolean isResponseStatusAcceptable(int statusCode) {
        return statusCode >= 200 && statusCode < 300;
    }

    private void prepareTuple(StormTaskTuple tuple) {
        tuple.getParameters().remove(RESOURCE_LINKS_COUNT);
        tuple.getParameters().remove(RESOURCE_URL);
    }

    private void prepareTuple(StormTaskTuple tuple, FileInfo edmFile) {
        prepareTuple(tuple);
        if (!edmFile.errors.isEmpty()) {
            tuple.addParameter(PluginParameterKeys.EXCEPTION_ERROR_MESSAGE, edmFile.errors);
            tuple.addParameter(PluginParameterKeys.UNIFIED_ERROR_MESSAGE, "media resource exception");
        }
    }

    private void emit(StormTaskTuple tuple) {
        outputCollector.emit(NOTIFICATION_STREAM_NAME, tuple.toStormTuple());
    }

}

/**
 * Information provided in tuple describing external link that should be checked by this bolt
 */
class ResourceInfo {
    String linkUrl;
    String edmUrl;
    int counter = 0;
}

/**
 * Information stored by this bold describing one ecloud file (edm file usually) that is being checked by this bolt
 */
class FileInfo {
    FileInfo(String fileUrl, int expectedNumberOfLinks, int linksChecked) {
        this.fileUrl = fileUrl;
        this.expectedNumberOfLinks = expectedNumberOfLinks;
        this.linksChecked = linksChecked;
    }

    String fileUrl;
    int expectedNumberOfLinks;
    int linksChecked;
    String errors = "";
}