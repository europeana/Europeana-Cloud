package eu.europeana.cloud.swiftmigrate.multitread;

import eu.europeana.cloud.swiftmigrate.SwiftMigrationDAO;
import eu.europeana.cloud.swiftmigrate.SwiftMigrator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;

/**
 * Manges creation of jobs and executing them.
 */
public class JobsController {

  public static Logger logger = LoggerFactory.getLogger(JobsController.class);
  Set<String> fileNames;

  final private SwiftMigrationDAO dao;
  final private SwiftMigrator swiftMigrator;
  final private int threadNumber;


  public JobsController(Set<String> fileNames, SwiftMigrationDAO dao, SwiftMigrator swiftMigrator,
      final int threadNumber) {
    this.fileNames = fileNames;
    this.dao = dao;
    this.swiftMigrator = swiftMigrator;
    this.threadNumber = threadNumber;
  }


  /**
   * Execute jobs.
   *
   * @throws InterruptedException
   * @throws ExecutionException
   */
  public void run()
      throws InterruptedException, ExecutionException {
    final ExecutorService executorService = Executors.newFixedThreadPool(threadNumber);
    final Set<Callable<String>> copyJobs = new HashSet<Callable<String>>();
    for (String fileName : fileNames) {
      copyJobs.add(new CopyFileJob(fileName, dao, swiftMigrator));
    }
    try {
      final List<Future<String>> futures = executorService.invokeAll(copyJobs);
      long copiedFileNumber = 0;
      for (Future<String> future : futures) {
        if (future.get().equalsIgnoreCase("ok")) {
          copiedFileNumber++;
        }
      }
      logger.info("Copied file number: " + copiedFileNumber);
    } catch (InterruptedException ex) {
      logger.error("Processing problem: ", ex.getMessage());
    }
    executorService.shutdown();
  }

}
