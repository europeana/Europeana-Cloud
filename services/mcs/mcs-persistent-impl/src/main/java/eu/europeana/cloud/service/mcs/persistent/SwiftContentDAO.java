package eu.europeana.cloud.service.mcs.persistent;

import com.google.common.io.BaseEncoding;
import com.google.common.io.ByteStreams;
import eu.europeana.cloud.common.model.File;
import eu.europeana.cloud.service.mcs.exception.FileAlreadyExistsException;
import eu.europeana.cloud.service.mcs.exception.FileContentHashMismatchException;
import eu.europeana.cloud.service.mcs.exception.FileNotExistsException;
import eu.europeana.cloud.service.mcs.exception.WrongContentRangeException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import org.springframework.stereotype.Repository;

/**
 *
 * @author sielski
 */
@Repository
public class SwiftContentDAO {

	public void putContent(String objectId, File file, InputStream data)
			throws IOException {
		throw new UnsupportedOperationException("Not implemented");
	}


	public void getContent(String objectId, long rangeStart, long rangeEnd, OutputStream os)
			throws IOException {
		throw new UnsupportedOperationException("Not implemented");

	}


	public void copyContent(String sourceObjectId, String trgObjectId) {
		throw new UnsupportedOperationException("Not implemented");

	}


	public void deleteContent(String objectId)
			throws FileNotExistsException {
		throw new UnsupportedOperationException("Not implemented");

	}


}
