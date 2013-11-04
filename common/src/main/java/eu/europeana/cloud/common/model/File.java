package eu.europeana.cloud.common.model;

import java.net.URI;

import javax.xml.bind.annotation.XmlRootElement;

/**
 * File
 */
@XmlRootElement
public class File {

	private String fileName;

	private String mimeType;

	private String md5;

	private String date;

	private long contentLength;

	private URI contentUri;

	public File() {
	}

	public File(String fileName, String mimeType, String md5, String date,
			long contentLength, URI contentUri) {
		super();
		this.fileName = fileName;
		this.mimeType = mimeType;
		this.md5 = md5;
		this.date = date;
		this.contentLength = contentLength;
		this.contentUri = contentUri;
	}
	
	public File(final File file){
		this(file.getFileName(), file.getMimeType(), file.getMd5(), file.getDate(), file.getContentLength(), file.getContentUri());
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

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ (int) (contentLength ^ (contentLength >>> 32));
		result = prime * result
				+ ((contentUri == null) ? 0 : contentUri.hashCode());
		result = prime * result + ((date == null) ? 0 : date.hashCode());
		result = prime * result
				+ ((fileName == null) ? 0 : fileName.hashCode());
		result = prime * result + ((md5 == null) ? 0 : md5.hashCode());
		result = prime * result
				+ ((mimeType == null) ? 0 : mimeType.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		File other = (File) obj;
		if (contentLength != other.contentLength)
			return false;
		if (contentUri == null) {
			if (other.contentUri != null)
				return false;
		} else if (!contentUri.equals(other.contentUri))
			return false;
		if (date == null) {
			if (other.date != null)
				return false;
		} else if (!date.equals(other.date))
			return false;
		if (fileName == null) {
			if (other.fileName != null)
				return false;
		} else if (!fileName.equals(other.fileName))
			return false;
		if (md5 == null) {
			if (other.md5 != null)
				return false;
		} else if (!md5.equals(other.md5))
			return false;
		if (mimeType == null) {
			if (other.mimeType != null)
				return false;
		} else if (!mimeType.equals(other.mimeType))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "File [fileName=" + fileName + ", mimeType=" + mimeType
				+ ", md5=" + md5 + ", date=" + date + ", contentLength="
				+ contentLength + ", contentUri=" + contentUri + "]";
	}

}
