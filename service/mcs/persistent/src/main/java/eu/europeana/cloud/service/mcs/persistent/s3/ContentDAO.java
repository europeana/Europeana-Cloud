package eu.europeana.cloud.service.mcs.persistent.s3;

import eu.europeana.cloud.service.mcs.exception.FileAlreadyExistsException;
import eu.europeana.cloud.service.mcs.exception.FileNotExistsException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Content storage DAOs interface.
 */
public interface ContentDAO {

  /**
   * Copies content of one storage object to another.
   *
   * @param sourceObjectId name of the source storage object
   * @param trgObjectId name of the target storage object
   * @throws FileNotExistsException if source object does not exist in the storage
   * @throws FileAlreadyExistsException if file already exists
   * @throws IOException if general I/O exception occurs
   */
  void copyContent(String sourceObjectId, String trgObjectId)
      throws FileNotExistsException, FileAlreadyExistsException, IOException;


  /**
   * Deletes storage object identified by fileName.
   *
   * @param fileName name of the object to be deleted
   * @throws FileNotExistsException if object does not exist in the storage
   */
  void deleteContent(String fileName) throws FileNotExistsException;


  /**
   * Retrieves content of file from storage. Can retrieve range of bytes of the file.
   *
   * @param fileName name of the file to retrieve
   * @param start first offset included in the response. If equal to -1, ignored.
   * @param end last offset included in the response (inclusive). If equal to -1, ignored.
   * @param os outputstream the content is written to
   * @throws IOException if an I/O error occurs
   * @throws FileNotExistsException if object does not exist in the storage
   */
  void getContent(String fileName, long start, long end, OutputStream os) throws IOException, FileNotExistsException;


  /**
   * Puts given content to storage under given fileName. Counts and returns content length and md5 checksum from given data.
   *
   * @param fileName name of the file
   * @param data content of file to be saved
   * @return md5 and content length
   * @throws IOException if an I/O error occurs
   */
  PutResult putContent(String fileName, InputStream data) throws IOException;

}
