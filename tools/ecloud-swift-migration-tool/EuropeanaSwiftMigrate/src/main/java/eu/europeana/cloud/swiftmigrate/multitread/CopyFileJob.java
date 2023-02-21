package eu.europeana.cloud.swiftmigrate.multitread;

import eu.europeana.cloud.service.mcs.exception.FileAlreadyExistsException;
import eu.europeana.cloud.service.mcs.exception.FileNotExistsException;
import eu.europeana.cloud.swiftmigrate.SwiftMigrationDAO;
import eu.europeana.cloud.swiftmigrate.SwiftMigrator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;

/**
 * Job Copy single file from one container to another.
 */
public class CopyFileJob implements Callable<String> {

  public final static Logger logger = LoggerFactory.getLogger(CopyFileJob.class);

  final private String oldFileName;
  final private SwiftMigrationDAO dao;
  final private SwiftMigrator swiftMigrator;


  public CopyFileJob(String oldFileName, SwiftMigrationDAO dao, SwiftMigrator swiftMigrator) {
    this.oldFileName = oldFileName;
    this.dao = dao;
    this.swiftMigrator = swiftMigrator;
  }


  public String changeFile() {
    try {
      final String newFileName = swiftMigrator.nameConversion(oldFileName);
      if (newFileName != null) {
        dao.copyFile(oldFileName, newFileName);
        logger.info("copy file " + oldFileName + " => " + newFileName);
      }
      return "ok";
    } catch (FileNotExistsException | FileAlreadyExistsException ex) {

      logger.error("Problem with copy file:", ex);
    }
    return "failure";
  }


  @Override
  public String call() {
    return changeFile();
  }

}
