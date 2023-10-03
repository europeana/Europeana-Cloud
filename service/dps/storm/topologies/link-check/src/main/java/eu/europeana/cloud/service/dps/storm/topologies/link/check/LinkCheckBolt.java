package eu.europeana.cloud.service.dps.storm.topologies.link.check;

import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.BoltInitializationException;
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
  transient Map<String, FileInfo> cache;

  private transient LinkChecker linkChecker;

  @Override
  protected boolean ignoreDeleted() {
    return false;
  }

  @Override
  public void prepare() {
    try {
      final MediaProcessorFactory processorFactory = new MediaProcessorFactory();
      linkChecker = processorFactory.createLinkChecker();
      cache = new HashMap<>(CACHE_SIZE);
    } catch (Exception e) {
      LOGGER.error("error while initializing Link checker {}", e.getCause(), e);
      throw new BoltInitializationException("error while initializing Link checker", e);
    }

  }

  /**
   * Performs link checking for given tuple
   *
   * @param tuple tuple that will be used for link checking
   */
  @Override
  public void execute(Tuple anchorTuple, StormTaskTuple tuple) {
    if (tuple.isMarkedAsDeleted()) {
        emitSuccessNotification(anchorTuple, tuple, "",
                "Record deleted - no links were checked.");
        outputCollector.ack(anchorTuple);
        return;
    }

    var resourceInfo = readResourceInfoFromTuple(tuple);
    if (!hasLinksForCheck(resourceInfo)) {
        emitSuccessNotification(anchorTuple, tuple, "",
                "The EDM file has no resources");
        outputCollector.ack(anchorTuple);
    } else {
      FileInfo edmFile = checkProvidedLink(tuple, resourceInfo);
      edmFile.addSourceTuple(anchorTuple);
      if (isFileFullyProcessed(edmFile)) {
        cache.remove(edmFile.fileUrl);
        if (edmFile.errors == null || edmFile.errors.isEmpty()) {
            emitSuccessNotification(anchorTuple, tuple, "", "");
        } else {
            emitSuccessNotification(anchorTuple, tuple, "", "", "resource exception", edmFile.errors);
        }
        ackAllSourceTuplesForFile(edmFile);
      }
    }
  }

  private void ackAllSourceTuplesForFile(FileInfo edmFile) {
    for (Tuple tuple : edmFile.sourceTuples) {
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

  private FileInfo checkProvidedLink(StormTaskTuple tuple, ResourceInfo resourceInfo) {
    FileInfo edmFile = takeFileFromCache(resourceInfo);
    if (edmFile == null || (edmFile.taskId != tuple.getTaskId())) {
      edmFile = new FileInfo(tuple.getTaskId(), resourceInfo.edmUrl, resourceInfo.expectedSize, 0,
          tuple.getRecordAttemptNumber());
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
      if (fileInfo.errors == null || fileInfo.errors.isEmpty()) {
        fileInfo.errors = error;
      } else {
        fileInfo.errors = fileInfo.errors + "," + error;
      }
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
    return cachedEdmFile.attemptNumber < tuple.getRecordAttemptNumber();
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

  long taskId;
  String fileUrl;
  int expectedNumberOfLinks;
  int linksChecked;
  int attemptNumber;
  String errors = "";
  List<Tuple> sourceTuples = new ArrayList<>();

  FileInfo(long taskId, String fileUrl, int expectedNumberOfLinks, int linksChecked, int attemptNumber) {
    this.taskId = taskId;
    this.fileUrl = fileUrl;
    this.expectedNumberOfLinks = expectedNumberOfLinks;
    this.linksChecked = linksChecked;
    this.attemptNumber = attemptNumber;
  }

  void addSourceTuple(Tuple anchorTuple) {
    sourceTuples.add(anchorTuple);
  }
}
