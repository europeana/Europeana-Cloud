package eu.europeana.cloud.common.model;

import static eu.europeana.cloud.service.mcs.Storage.OBJECT_STORAGE;

import eu.europeana.cloud.service.mcs.Storage;
import jakarta.xml.bind.annotation.XmlRootElement;
import lombok.EqualsAndHashCode;

import java.net.URI;

/**
 * Metadata of a file (content) in a certain representation version of a record.
 */
@XmlRootElement
@EqualsAndHashCode
public class File {

  /**
   * Name of a file - primary identifier of a file inside a certain representation of a record.
   */
  private String fileName;

  /**
   * Mime type of a content.
   */
  private String mimeType;

  /**
   * MD5 hash of a content.
   */
  private String md5;

  /**
   * Last content modification date.
   */
  private String date;

  /**
   * Number of bytes in content.
   */
  private long contentLength;

  /**
   * Uri to content.
   */
  private URI contentUri;

  /**
   * Indicate where file is stored. </br>
   * <p>
   * Default value {@link Storage#OBJECT_STORAGE}
   */
  private Storage fileStorage;


  /**
   * Creates new empty instance of {@link File}
   */
  public File() {
    this.fileStorage = OBJECT_STORAGE;
  }


  /**
   * Creates new instance of {@link File} class based on the provided parameters
   *
   * @param fileName file name
   * @param mimeType mime type
   * @param md5 md5
   * @param date lat modification date
   * @param contentLength content length
   * @param contentUri content URI
   * @param fileStorage file location (see {@link Storage})
   */
  public File(String fileName, String mimeType, String md5, String date,
      long contentLength, URI contentUri, Storage fileStorage) {
    super();
    this.fileName = fileName;
    this.mimeType = mimeType;
    this.md5 = md5;
    this.date = date;
    this.contentLength = contentLength;
    this.contentUri = contentUri;
    this.fileStorage = fileStorage;
  }

  /**
   * Creates new instance of {@link File} class based on the provided parameters
   *
   * @param fileName file name
   * @param mimeType mime type
   * @param md5 md5
   * @param date last modification date
   * @param contentLength content length
   * @param contentUri content URI
   */
  public File(String fileName, String mimeType, String md5, String date,
      long contentLength, URI contentUri) {
    this(fileName, mimeType, md5, date, contentLength, contentUri, OBJECT_STORAGE);
  }


  /**
   * Creates new instance of {@link File} class based on the provided parameters
   *
   * @param file instance of the {@link File} class that will be used to construct new object
   */
  public File(final File file) {
    this(file.getFileName(), file.getMimeType(), file.getMd5(), file.getDate(), file.getContentLength(), file.
        getContentUri(), file.fileStorage);
  }


  public String getFileName() {
    return fileName;
  }


  public void setFileName(String fileName) {
    this.fileName = fileName;
  }


  public String getMimeType() {
    return mimeType;
  }


  public void setMimeType(String mimeType) {
    this.mimeType = mimeType;
  }


  public String getMd5() {
    return md5;
  }


  public void setMd5(String md5) {
    this.md5 = md5;
  }


  public String getDate() {
    return date;
  }


  public void setDate(String date) {
    this.date = date;
  }


  public long getContentLength() {
    return contentLength;
  }


  public void setContentLength(long contentLength) {
    this.contentLength = contentLength;
  }


  public URI getContentUri() {
    return contentUri;
  }


  public void setContentUri(URI contentUri) {
    this.contentUri = contentUri;
  }


  public Storage getFileStorage() {
    return fileStorage;
  }

  public void setFileStorage(Storage fileStorage) {
    this.fileStorage = fileStorage;
  }

  @Override
  public String toString() {
    return "File{" +
        "fileName='" + fileName + '\'' +
        ", mimeType='" + mimeType + '\'' +
        ", md5='" + md5 + '\'' +
        ", date='" + date + '\'' +
        ", contentLength=" + contentLength +
        ", contentUri=" + contentUri +
        ", fileStorage=" + fileStorage +
        '}';
  }

}
