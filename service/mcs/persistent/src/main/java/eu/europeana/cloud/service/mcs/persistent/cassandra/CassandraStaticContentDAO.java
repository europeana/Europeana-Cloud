package eu.europeana.cloud.service.mcs.persistent.cassandra;

import com.datastax.driver.core.*;
import com.datastax.driver.core.utils.Bytes;
import com.google.common.io.BaseEncoding;
import com.google.common.io.CountingInputStream;
import com.google.common.primitives.Ints;
import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.common.annotation.Retryable;
import eu.europeana.cloud.service.mcs.exception.FileNotExistsException;
import eu.europeana.cloud.service.mcs.persistent.s3.ContentDAO;
import eu.europeana.cloud.service.mcs.persistent.s3.PutResult;
import eu.europeana.cloud.service.mcs.persistent.util.QueryTracer;
import jakarta.annotation.PostConstruct;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.ByteArrayOutputStream;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

/**
 * Provides content DAO operations for Cassandra.
 *
 * @author krystian.
 */
public class CassandraStaticContentDAO implements ContentDAO {

  private static final String MSG_FILE_NOT_EXISTS = "File with md5 %s not exists";
  private static final String MSG_CANNOT_GET_INSTANCE_OF_MD_5 = "Cannot get instance of MD5 but such algorithm should be provided";

  private final CassandraConnectionProvider connectionProvider;
  private final StreamCompressor streamCompressor = new StreamCompressor();
  private PreparedStatement insertStatement;
  private PreparedStatement selectStatement;
  private PreparedStatement selectFileNamesAndUpdatedStatement;
  private PreparedStatement deleteStatement;
  private PreparedStatement deleteStaticColumnStatement;

  public CassandraStaticContentDAO(CassandraConnectionProvider connectionProvider) {
    this.connectionProvider = connectionProvider;
  }

  /**
   * @inheritDoc
   */
  @Override
  @Retryable
  public void deleteContent(String md5, String fileName) throws FileNotExistsException {
    ResultSet rs = executeQueryWithLogger(deleteStatement.bind(md5, fileName));
    if (!rs.wasApplied()) {
      throw new FileNotExistsException(String.format(MSG_FILE_NOT_EXISTS, md5));
    }

    rs = executeQueryWithLogger(selectFileNamesAndUpdatedStatement.bind(md5));
    List<Row> rows = rs.all();
    if (rows.size() == 1) {
      Row row = rows.getFirst();
      if (row.getString("filename") == null) {
        executeQueryWithLogger(deleteStaticColumnStatement.bind(md5, row.getTimestamp("updated")));
      }
    }
  }


  /**
   * @inheritDoc
   */
  @Override
  public void getContent(String fileName, String md5, long start, long end, OutputStream result) throws IOException, FileNotExistsException {
    ResultSet rs = executeQueryWithLogger(selectStatement.bind(md5));

    Row row = rs.one();
    if (row == null) {
      throw new FileNotExistsException(String.format(MSG_FILE_NOT_EXISTS, md5));
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
  @Retryable
  public PutResult putContent(String fileName, InputStream data) throws IOException {
    CountingInputStream countingInputStream = new CountingInputStream(data);
    DigestInputStream md5DigestInputStream = prepareMd5DigestStream(countingInputStream);
    ByteBuffer wrappedBytes = ByteBuffer.wrap(streamCompressor.compress(md5DigestInputStream));
    String md5 = BaseEncoding.base16().lowerCase().encode(md5DigestInputStream.getMessageDigest().digest());
    executeQueryWithLogger(insertStatement.bind(md5, fileName, wrappedBytes, new Date()));
    Long contentLength = countingInputStream.getCount();
    return new PutResult(md5, contentLength);
  }

  @PostConstruct
  private void prepareStatements() {
    Session s = connectionProvider.getSession();
    insertStatement = s.prepare("INSERT INTO files_content_v2 (md5, fileName, data, updated) VALUES (?,?,?,?) IF NOT EXISTS");

    selectStatement = s.prepare("SELECT data FROM files_content_v2 WHERE md5 = ?;");

    selectFileNamesAndUpdatedStatement = s.prepare("SELECT filename, updated FROM files_content_v2 WHERE md5 = ?;");

    deleteStatement = s.prepare("DELETE FROM files_content_v2 WHERE md5 =? AND fileName = ? IF EXISTS;");

    deleteStaticColumnStatement = s.prepare("DELETE data, updated FROM files_content_v2 WHERE md5 = ? IF updated <= ?;");
  }


  private ResultSet executeQueryWithLogger(BoundStatement boundStatement) {
    ResultSet rs = connectionProvider.getSession().execute(boundStatement);
    QueryTracer.logConsistencyLevel(boundStatement, rs);
    return rs;
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
