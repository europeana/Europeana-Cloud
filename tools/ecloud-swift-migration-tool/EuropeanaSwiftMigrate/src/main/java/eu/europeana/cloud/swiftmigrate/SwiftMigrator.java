package eu.europeana.cloud.swiftmigrate;

import eu.europeana.cloud.service.mcs.persistent.s3.SimpleSwiftConnectionProvider;
import eu.europeana.cloud.swiftmigrate.multitread.JobsController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;

/**
 * Class copy all files from source container to target container.
 */
public abstract class SwiftMigrator {

  public final static Logger logger = LoggerFactory.getLogger(Migrator.class);


  /**
   * Method convert file name.
   *
   * @param input {@link String}
   * @return converted {@link String}
   */
  public abstract String nameConversion(final String input);


  private final int defaultNumberOfThread;


  public SwiftMigrator() {
    defaultNumberOfThread = 10;
  }


  public void chagngeFileName(final SimpleSwiftConnectionProvider sourceProvider,
      final SimpleSwiftConnectionProvider targetProvider) {
    final SwiftMigrationDAO dao = new SwiftMigrationDAO(sourceProvider, targetProvider);
    final Set<String> fileList = filterFiles(dao.getFilesList());
    delegateTaskToTreadPool(fileList, dao);
    sourceProvider.closeConnections();
    targetProvider.closeConnections();
  }


  /**
   * Delegate task to thread pool.
   *
   * @param fileList
   * @param dao
   */
  private void delegateTaskToTreadPool(final Set<String> fileList, final SwiftMigrationDAO dao) {
    final JobsController controllerThread = new JobsController(fileList, dao, this, defaultNumberOfThread);
    try {
      controllerThread.run();
    } catch (InterruptedException | ExecutionException ex) {
      logger.error("Error occured", ex);
    }
  }


  /**
   * Filter file set if mach to conversion.
   *
   * @param fileSet
   * @return
   */
  private Set<String> filterFiles(final Set<String> fileSet) {
    final Set<String> filteredFileSet = new HashSet<>();
    for (String s : fileSet) {
      if (nameConversion(s) != null) {
        filteredFileSet.add(s);
      }
    }
    logger.info("Container file number :" + fileSet.size());
    logger.info("To copy file number :" + filteredFileSet.size());
    return filteredFileSet;
  }
}
