package eu.europeana.cloud.service.mcs.persistent.s3;

import eu.europeana.cloud.service.mcs.exception.FileNotExistsException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Content storage DAOs interface.
 */
public interface ContentDAO {

  /**
   * Deletes storage object identified by fileName.
   *
   * @param md5 md5 checksum of file
   * @param fileName name of the object to be deleted
   * @throws FileNotExistsException if object does not exist in the storage
   */
  void deleteContent(String md5, String fileName) throws FileNotExistsException;


  /**
   * Retrieves content of file from storage. Can retrieve range of bytes of the file.
   *
   * @param fileName name of the file to retrieve
   * @param md5 md5 checksum
   * @param start first offset included in the response. If equal to -1, ignored.
   * @param end last offset included in the response (inclusive). If equal to -1, ignored.
   * @param os outputstream the content is written to
   * @throws IOException if an I/O error occurs
   * @throws FileNotExistsException if object does not exist in the storage
   */
  void getContent(String fileName, String md5, long start, long end, OutputStream os) throws IOException, FileNotExistsException;




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
