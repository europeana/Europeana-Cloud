package eu.europeana.cloud.service.dps.storm.topologies.link.check;

import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.metis.mediaprocessing.LinkChecker;
import eu.europeana.metis.mediaprocessing.MediaProcessorFactory;
import lombok.ToString;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by pwozniak on 2/5/19
 */
public class LinkCheckBolt extends AbstractDpsBolt {

    private static final Logger LOGGER = LoggerFactory.getLogger(LinkCheckBolt.class);

    private static final int CACHE_SIZE = 1024;
    private transient Map<String, FileInfo> cache ;

    private transient LinkChecker linkChecker;


    @Override
    public void prepare() {
        try {
            cache = new HashMap<>(CACHE_SIZE);
            final MediaProcessorFactory processorFactory = new MediaProcessorFactory();
            linkChecker = processorFactory.createLinkChecker();
        } catch (Exception e) {
            LOGGER.error("error while initializing Link checker {}", e.getCause(), e);
            throw new RuntimeException("error while initializing Link checker", e);
        }

    }

    /**
     * Performs link checking for given tuple
     *
     * @param tuple tuple that will be used for link checking
     */
    @Override
    public void execute(Tuple anchorTuple, StormTaskTuple tuple) {
        ResourceInfo resourceInfo = readResourceInfoFromTuple(tuple);
        if (!hasLinksForCheck(resourceInfo)) {
            emitSuccessNotification(anchorTuple, tuple.getTaskId(), tuple.getFileUrl(), "", "The EDM file has no resources", "");
            outputCollector.ack(anchorTuple);
        } else {
            FileInfo edmFile = checkProvidedLink(resourceInfo, tuple);
            edmFile.addSourceTuple(anchorTuple);
            if (isFileFullyProcessed(edmFile)) {
                cache.remove(edmFile.fileUrl);
                if (edmFile.errors == null || edmFile.errors.isEmpty())
                    emitSuccessNotification(anchorTuple, tuple.getTaskId(), tuple.getFileUrl(), "", "", "");
                else
                    emitSuccessNotification(anchorTuple, tuple.getTaskId(), tuple.getFileUrl(), "", "", "", "resource exception", edmFile.errors);
                ackAllSourceTuplesForFile(edmFile);
            }
        }
    }

    private void ackAllSourceTuplesForFile(FileInfo edmFile) {
        for(Tuple tuple: edmFile.sourceTupples){
            outputCollector.ack(tuple);
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

    private FileInfo checkProvidedLink(ResourceInfo resourceInfo, StormTaskTuple tuple) {
        FileInfo edmFile = takeFileFromCache(resourceInfo);
        if (edmFile == null) {
            edmFile = new FileInfo(tuple.getTaskId(), resourceInfo.edmUrl, resourceInfo.expectedSize, 0, tuple.getRecordAttemptNumber());
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

    protected void cleanInvalidData(StormTaskTuple tuple) {
        ResourceInfo resourceInfo = readResourceInfoFromTuple(tuple);
        FileInfo cachedEdmFile = takeFileFromCache(resourceInfo);

        if ((cachedEdmFile != null) && cachedFileIsFromPreviousAttempt(tuple, cachedEdmFile)) {
            cache.remove(resourceInfo.edmUrl);
            LOGGER.info("Cleared cached file {} for resource {}, it was from previous attempt.", cachedEdmFile, resourceInfo);
        }

    }

    private boolean cachedFileIsFromPreviousAttempt(StormTaskTuple tuple, FileInfo cachedEdmFile) {
        return (cachedEdmFile.taskId != tuple.getTaskId()) || (cachedEdmFile.attempNumber < tuple.getRecordAttemptNumber());
    }
}

/**
 * Information provided in tuple describing external link that should be checked by this bolt
 */
@ToString
class ResourceInfo {
    String linkUrl;
    String edmUrl;
    int expectedSize;
}

/**
 * Information stored by this bold describing one ecloud file (edm file usually) that is being checked by this bolt
 */
@ToString
class FileInfo {

    FileInfo(long taskId, String fileUrl, int expectedNumberOfLinks, int linksChecked, int attempNumber) {
        this.taskId = taskId;
        this.fileUrl = fileUrl;
        this.expectedNumberOfLinks = expectedNumberOfLinks;
        this.linksChecked = linksChecked;
        this.attempNumber = attempNumber;
    }

    long taskId;
    String fileUrl;
    int expectedNumberOfLinks;
    int linksChecked;
    int attempNumber;
    String errors = "";
    List<Tuple> sourceTupples=new ArrayList<>();

    void addSourceTuple(Tuple anchorTuple) {
        sourceTupples.add(anchorTuple);
    }
}