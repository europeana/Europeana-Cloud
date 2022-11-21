package eu.europeana.cloud.http.service;

import eu.europeana.cloud.http.common.CompressionFileExtension;
import eu.europeana.cloud.http.common.UnpackingServiceFactory;
import eu.europeana.cloud.http.exceptions.CompressionExtensionNotRecognizedException;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.rauschig.jarchivelib.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Iterator;


public class GzUnpackingService implements FileUnpackingService {

  public static final String TAR = ".tar";

  public void unpackFile(final String zipFile, final String destinationFolder)
      throws CompressionExtensionNotRecognizedException, IOException {
    String[] extensions = CompressionFileExtension.getExtensionValues();
    unpackFile(zipFile, destinationFolder, extensions);
  }

  private void unpackFile(final String compressedFile, final String destinationFolder, final String[] extensions)
      throws CompressionExtensionNotRecognizedException, IOException {

    File destination = new File(destinationFolder);
    if (FilenameUtils.getName(compressedFile).contains(TAR)
        || (FilenameUtils.getExtension(compressedFile)).equals(CompressionFileExtension.TGZIP.getExtension())) {
      File newDestination = extractTarGzipArchive(compressedFile, destination);
      Iterator<File> files = FileUtils.iterateFiles(newDestination, extensions, true);
      while (files.hasNext()) {
        File file = files.next();
        String extension = FilenameUtils.getExtension(file.getName());
        UnpackingServiceFactory.createUnpackingService(extension)
                               .unpackFile(file.getAbsolutePath(), file.getParent() + File.separator);
      }
    } else {
      try (GzipCompressorInputStream inputStream = new GzipCompressorInputStream(new FileInputStream(compressedFile));
          FileOutputStream fileOutputStream = new FileOutputStream(new File(FilenameUtils.removeExtension(compressedFile)))) {
        IOUtils.copy(inputStream, fileOutputStream);
      }
    }
  }

  private File extractTarGzipArchive(String compressedFile, File destination) throws IOException {
    File archive = new File(compressedFile);
    Archiver archiver = ArchiverFactory.createArchiver(ArchiveFormat.TAR, CompressionType.GZIP);
    archiver.extract(archive, destination);
    String fileName = getFileName(compressedFile);
    return getDestinationFolder(destination.getPath(), fileName);
  }

  private String getFileName(String fileLocation) {
    return FilenameUtils.getName(FilenameUtils.removeExtension(FilenameUtils.removeExtension(fileLocation)));
  }

  private File getDestinationFolder(String destination, String fileName) {
    return new File(destination + File.separator + fileName);
  }

}
