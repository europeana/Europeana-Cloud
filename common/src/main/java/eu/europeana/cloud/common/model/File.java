package eu.europeana.cloud.common.model;

import static eu.europeana.cloud.service.mcs.Storage.OBJECT_STORAGE;

import eu.europeana.cloud.service.mcs.Storage;
import java.net.URI;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * Metadata of a file (content) in a certain representation version of a record.
 */
@XmlRootElement
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
   * @param date lat modification date
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
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || this.getClass() != o.getClass()) {
      return false;
    }

    File file = (File) o;

    if (contentLength != file.contentLength) {
      return false;
    }
    if (fileStorage != file.fileStorage) {
      return false;
    }
    if (fileName != null ? !fileName.equals(file.fileName) : (file.fileName != null)) {
      return false;
    }
    if (mimeType != null ? !mimeType.equals(file.mimeType) : (file.mimeType != null)) {
      return false;
    }
    if (md5 != null ? !md5.equals(file.md5) : (file.md5 != null)) {
      return false;
    }
    if (date != null ? !date.equals(file.date) : (file.date != null)) {
      return false;
    }
    return contentUri != null ? contentUri.equals(file.contentUri) : (file.contentUri == null);

  }

  @Override
  public int hashCode() {
    int result = fileName != null ? fileName.hashCode() : 0;
    result = 31 * result + (mimeType != null ? mimeType.hashCode() : 0);
    result = 31 * result + (md5 != null ? md5.hashCode() : 0);
    result = 31 * result + (date != null ? date.hashCode() : 0);
    result = 31 * result + (int) (contentLength ^ (contentLength >>> 32));
    result = 31 * result + (contentUri != null ? contentUri.hashCode() : 0);
    result = 31 * result + (fileStorage != null ? fileStorage.hashCode() : 0);
    return result;
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
