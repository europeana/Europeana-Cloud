package eu.europeana.cloud.swiftmigrate;

import eu.europeana.cloud.service.mcs.exception.FileAlreadyExistsException;
import eu.europeana.cloud.service.mcs.exception.FileNotExistsException;
import eu.europeana.cloud.service.mcs.persistent.swift.SimpleSwiftConnectionProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class copy all files from source container to target container.
 * 
 */
public abstract class SwiftMigrator {

    public static Logger logger = LoggerFactory.getLogger(Migrator.class);


    /**
     * Method convert file name.
     * 
     * @param input
     *            {@link String}
     * @return converted {@link String}
     */
    protected abstract String nameConversion(final String input);


    public void chagngeFileName(final SimpleSwiftConnectionProvider sourceProvider,
            final SimpleSwiftConnectionProvider targetProvider) {

        final SwiftMigrationDAO dao = new SwiftMigrationDAO(sourceProvider, targetProvider);
        for (String oldFileName : dao.getFilesList()) {
            try {
                final String newFileName = nameConversion(oldFileName);
                if (newFileName != null) {
                    dao.copyFile(oldFileName, newFileName);
                    logger.info("CopyFile " + oldFileName + " => " + newFileName);
                }
            } catch (FileNotExistsException ex) {
                logger.error("File is not exist.", ex);
            } catch (FileAlreadyExistsException ex) {
                logger.error("File already exist.", ex);
            }
        }
        sourceProvider.closeConnections();
        targetProvider.closeConnections();
    }
}
