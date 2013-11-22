package eu.europeana.cloud.service.mcs.persistent;

import com.google.common.io.ByteStreams;
import eu.europeana.cloud.common.model.File;
import eu.europeana.cloud.service.mcs.exception.FileNotExistsException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.commons.codec.binary.Hex;
import org.jclouds.blobstore.options.GetOptions;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.domain.Blob;
import org.jclouds.io.Payload;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

/**
 * 
 * @author olanowak
 */
@Repository
public class SwiftContentDAO
{

	@Autowired
	private SwiftConnectionProvider connectionProvider;


	/**
	 * Puts givent content to storage under given fileName. Counts content length and md5 checksum and updates file with
	 * it.
	 * 
	 * @param fileName name of the file
	 * @param file file to be saved
	 * @param data content of file to be saved
	 * @throws IOException if an I/O error occurs
	 */
	public void putContent(String fileName, File file, InputStream data)
		throws IOException
	{

		BlobStore blobStore = connectionProvider.getBlobStore();
		String container = connectionProvider.getContainer();

		Blob blob = blobStore.blobBuilder(fileName).name(fileName).payload(data).calculateMD5().build();
		blobStore.putBlob(container, blob);

		file.setMd5(new String(Hex.encodeHex(blob.getMetadata().getContentMetadata().getContentMD5())));
		file.setContentLength(blob.getMetadata().getContentMetadata().getContentLength());
	}


	/**
	 * Retrieves content of file from storage. Can retrieve range od bytes of the file.
	 * 
	 * @param fileName name of the file to retrieve
	 * @param start first offset included in the response. If equal to -1, ignored.
	 * @param end last offset included in the response (inclusive). If equal to -1, ignored.
	 * @param os outputstream the content is written to
	 * @throws IOException if an I/O error occurs
	 * @throws FileNotExistsException if object does not exist in the storage
	 */
	public void getContent(String fileName, long start, long end, OutputStream os)
		throws IOException, FileNotExistsException
	{
		BlobStore blobStore = connectionProvider.getBlobStore();
		String container = connectionProvider.getContainer();

		if (!blobStore.blobExists(connectionProvider.getContainer(), fileName)) {
			throw new FileNotExistsException(String.format("File %s not exists", fileName));
		}

		GetOptions options = GetOptions.NONE;
		if (start > -1 && end > -1) {
			options = new GetOptions().range(start, end);
		}
		else if (start > -1 && end == -1) {
			options = new GetOptions().startAt(start);
		}
		else if (start == -1 && end > -1) {
			options = new GetOptions().range(0, end);
		}
		Payload payload = blobStore.getBlob(container, fileName, options).getPayload();
		if (payload != null) {
			ByteStreams.copy(payload.getInput(), os);
		}
	}


	/**
	 * Copies content of one storage object to another.
	 * 
	 * @param sourceObjectId name of the source storage object
	 * @param trgObjectId name of the target storage object
	 * @throws FileNotExistsException if source object does not exist in the storage
	 */
	public void copyContent(String sourceObjectId, String trgObjectId)
		throws FileNotExistsException
	{
		BlobStore blobStore = connectionProvider.getBlobStore();
		String container = connectionProvider.getContainer();
		if (!blobStore.blobExists(connectionProvider.getContainer(), sourceObjectId)) {
			throw new FileNotExistsException(String.format("File %s not exists", sourceObjectId));
		}
		Blob blob = blobStore.getBlob(container, sourceObjectId);
		Blob newBlob = blobStore.blobBuilder(trgObjectId).name(trgObjectId).payload(blob.getPayload()).build();
		blobStore.putBlob(container, newBlob);
	}


	/**
	 * Deletes storage object identifed by fileName.
	 * 
	 * @param fileName name of the object to be deleted
	 * @throws FileNotExistsException if object does not exist in the storage
	 */
	public void deleteContent(String fileName)
		throws FileNotExistsException
	{
		BlobStore blobStore = connectionProvider.getBlobStore();
		String container = connectionProvider.getContainer();
		if (!blobStore.blobExists(connectionProvider.getContainer(), fileName)) {
			throw new FileNotExistsException(String.format("File %s not exists", fileName));
		}
		blobStore.removeBlob(container, fileName);
	}
}
