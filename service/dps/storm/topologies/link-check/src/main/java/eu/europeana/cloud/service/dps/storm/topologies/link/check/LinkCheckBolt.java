package eu.europeana.cloud.service.dps.storm.topologies.link.check;

import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.metis.mediaprocessing.LinkChecker;
import eu.europeana.metis.mediaprocessing.MediaProcessorFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by pwozniak on 2/5/19
 */
public class LinkCheckBolt extends AbstractDpsBolt {

    private static final Logger LOGGER = LoggerFactory.getLogger(LinkCheckBolt.class);

    private static final int CACHE_SIZE = 1024;
    Map<String, FileInfo> cache = new HashMap<>(CACHE_SIZE);

    private LinkChecker linkChecker;


    @Override
    public void prepare() {
        try {
            final MediaProcessorFactory processorFactory = new MediaProcessorFactory();
            linkChecker = processorFactory.createLinkChecker();
        } catch (Exception e) {
            String errorMessage = "error while initializing Link checker";
            LOGGER.error(errorMessage + " " + e.getCause());
            throw new RuntimeException("error while initializing Link checker", e);
        }

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
            emitSuccessNotification(tuple.getTaskId(), tuple.getFileUrl(), "", "The EDM file has no resources", "");
        } else {
            FileInfo edmFile = checkProvidedLink(resourceInfo);
            if (isFileFullyProcessed(edmFile)) {
                removeFileFromCache(edmFile);
                if (edmFile.errors == null || edmFile.errors.isEmpty())
                    emitSuccessNotification(tuple.getTaskId(), tuple.getFileUrl(), "", "", "");
                else
                    emitSuccessNotification(tuple.getTaskId(), tuple.getFileUrl(), "", "", "", "resource exception", edmFile.errors);
            }
        }
    }

    private ResourceInfo readResourceInfoFromTuple(StormTaskTuple tuple) {
        ResourceInfo resourceInfo = new ResourceInfo();
        resourceInfo.expectedSize = Integer.parseInt(tuple.getParameter(PluginParameterKeys.RESOURCE_LINKS_COUNT));
        resourceInfo.edmUrl = tuple.getFileUrl();
        resourceInfo.linkUrl = tuple.getParameter(PluginParameterKeys.RESOURCE_URL);
        return resourceInfo;
    }

    private boolean hasLinksForCheck(ResourceInfo resourceInfo) {
        return resourceInfo.expectedSize > 0;
    }

    private FileInfo checkProvidedLink(ResourceInfo resourceInfo) {
        FileInfo edmFile = takeFileFromCache(resourceInfo);
        if (edmFile == null) {
            edmFile = new FileInfo(resourceInfo.edmUrl, resourceInfo.expectedSize, 0);
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
            linkChecker.performLinkChecking(resourceInfo.linkUrl);
        } catch (Exception e) {
            LOGGER.info("There was exception while checking the link: {}", resourceInfo.edmUrl);
            String error = e.getMessage() + " . Because of: " + e.getCause();
            if (fileInfo.errors == null || fileInfo.errors.isEmpty())
                fileInfo.errors = error;
            else
                fileInfo.errors = fileInfo.errors + "," + error;
        } finally {
            fileInfo.linksChecked++;
        }
    }

}

/**
 * Information provided in tuple describing external link that should be checked by this bolt
 */
class ResourceInfo {
    String linkUrl;
    String edmUrl;
    int expectedSize;
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