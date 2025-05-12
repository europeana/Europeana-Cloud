package eu.europeana.cloud.service.mcs.persistent.cassandra;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.utils.Bytes;
import com.google.common.io.BaseEncoding;
import com.google.common.io.CountingInputStream;
import com.google.common.primitives.Ints;
import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.common.annotation.Retryable;
import eu.europeana.cloud.service.mcs.exception.FileAlreadyExistsException;
import eu.europeana.cloud.service.mcs.exception.FileNotExistsException;
import eu.europeana.cloud.service.mcs.persistent.s3.ContentDAO;
import eu.europeana.cloud.service.mcs.persistent.s3.PutResult;
import eu.europeana.cloud.service.mcs.persistent.util.QueryTracer;
import jakarta.annotation.PostConstruct;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.ByteArrayOutputStream;

/**
 * Provides content DAO operations for Cassandra.
 *
 * @author krystian.
 */
public class CassandraContentDAO implements ContentDAO {

  private static final String MSG_FILE_NOT_EXISTS = "File %s not exists";
  private static final String MSG_FILE_ALREADY_EXISTS = "File %s already exists";
  private static final String MSG_CANNOT_GET_INSTANCE_OF_MD_5 = "Cannot get instance of MD5 but such algorithm should be provided";

  private final CassandraConnectionProvider connectionProvider;
  private final StreamCompressor streamCompressor = new StreamCompressor();
  private PreparedStatement insertStatement;
  private PreparedStatement selectStatement;
  private PreparedStatement deleteStatement;

  public CassandraContentDAO(CassandraConnectionProvider connectionProvider) {
    this.connectionProvider = connectionProvider;
  }


  /**
   * @inheritDoc
   */
  @Override
  @Retryable
  public void deleteContent(String md5, String fileName) throws FileNotExistsException {
    ResultSet rs = executeQueryWithLogger(deleteStatement.bind(fileName));
    if (!rs.wasApplied()) {
      throw new FileNotExistsException(String.format(MSG_FILE_NOT_EXISTS, fileName));
    }
  }

  /**
   * @inheritDoc
   */
  @Override
  @Retryable
  public void getContent(String md5, String fileName, long start, long end, OutputStream result)
          throws IOException, FileNotExistsException {

    ResultSet rs = executeQueryWithLogger(selectStatement.bind(fileName));

    Row row = rs.one();
    if (row == null) {
      throw new FileNotExistsException(String.format(MSG_FILE_NOT_EXISTS, fileName));
    }
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    try {
      ByteBuffer wrappedBytes = row.getBytes("data");
      streamCompressor.decompress(unwrap(wrappedBytes), os);
      copySelectBytes(os, start, end, result);
    } finally {
      IOUtils.closeQuietly(os);
    }

  }

  /**
   * @inheritDoc
   */
  @Override
  public void copyContent(String md5, String sourceFileName, String targetFileName)
          throws FileNotExistsException, FileAlreadyExistsException, IOException {

    try (ByteArrayOutputStream os = new ByteArrayOutputStream()) {
      getContent(md5, sourceFileName, -1, -1, os);
      checkIfObjectNotExists(targetFileName);
      putContent(targetFileName, new ByteArrayInputStream(os.toByteArray()));
    } catch (FileNotExistsException e) {
      throw new FileNotExistsException(String.format(MSG_FILE_NOT_EXISTS, sourceFileName));
    }
  }
  /**
   * @inheritDoc
   */
  @Override
  @Retryable
  public PutResult putContent(String fileName, InputStream data) throws IOException {
    CountingInputStream countingInputStream = new CountingInputStream(data);
    DigestInputStream md5DigestInputStream = prepareMd5DigestStream(countingInputStream);
    ByteBuffer wrappedBytes = ByteBuffer.wrap(streamCompressor.compress(md5DigestInputStream));
    executeQueryWithLogger(insertStatement.bind(fileName, wrappedBytes));
    String md5 = BaseEncoding.base16().lowerCase().encode(md5DigestInputStream.getMessageDigest().digest());
    Long contentLength = countingInputStream.getCount();
    return new PutResult(md5, contentLength);
  }

  @PostConstruct
  private void prepareStatements() {
    Session s = connectionProvider.getSession();
    insertStatement = s.prepare("INSERT INTO files_content (fileName, data) VALUES (?,?) IF NOT EXISTS");

    selectStatement = s.prepare("SELECT data FROM files_content WHERE fileName = ?;");

    deleteStatement = s.prepare("DELETE FROM files_content WHERE fileName = ? IF EXISTS;");
  }

  private ResultSet executeQueryWithLogger(BoundStatement boundStatement) {
    ResultSet rs = connectionProvider.getSession().execute(boundStatement);
    QueryTracer.logConsistencyLevel(boundStatement, rs);
    return rs;
  }

  private void checkIfObjectNotExists(String trgObjectId) throws FileAlreadyExistsException {
    ResultSet rs = executeQueryWithLogger(selectStatement.bind(trgObjectId));
    Row row = rs.one();
    if (row != null) {
      throw new FileAlreadyExistsException(String.format(MSG_FILE_ALREADY_EXISTS, trgObjectId));
    }
  }

  private void copySelectBytes(ByteArrayOutputStream input, long start, long end, OutputStream result) throws IOException {
    byte[] resultBytes;
    if (start < 0 && end < 0) {
      resultBytes = input.toByteArray();
    } else {
      resultBytes = getSelectedBytes(input.toByteArray(), start, end);
    }
    IOUtils.copy(new ByteArrayInputStream(resultBytes), result);
  }

  private byte[] unwrap(ByteBuffer wrappedBytes) {
    return Bytes.getArray(wrappedBytes);
  }

  private byte[] getSelectedBytes(byte[] bytes, long start, long end) {
    byte[] outputBytes;
    final int from = start > -1 ? Ints.checkedCast(start) : 0;
    final int to = end > -1 ? (Ints.checkedCast(end) + 1) : bytes.length;
    outputBytes = Arrays.copyOfRange(bytes, from, to);
    return outputBytes;
  }

  private DigestInputStream prepareMd5DigestStream(InputStream is) {
    try {
      @SuppressWarnings("java:S4790") //The md5 is used here not for security, but for as file checksum.
      // The meaningful here is collision probability, which is very low 1.47*10-29.
      // So we could use it here safety. Anyway we could not change algorithm without changing API and
      // rebuilding Cassandra DB with stored md5.
      MessageDigest md = MessageDigest.getInstance("MD5");
      return new DigestInputStream(is, md);
    } catch (NoSuchAlgorithmException ex) {
      throw new AssertionError(MSG_CANNOT_GET_INSTANCE_OF_MD_5, ex);
    }
  }
}
