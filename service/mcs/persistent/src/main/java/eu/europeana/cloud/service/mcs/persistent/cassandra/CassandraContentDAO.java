package eu.europeana.cloud.service.mcs.persistent.cassandra;

import com.datastax.driver.core.*;
import com.datastax.driver.core.utils.Bytes;
import com.google.common.io.BaseEncoding;
import com.google.common.io.CountingInputStream;
import com.google.common.primitives.Ints;
import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.service.mcs.exception.FileAlreadyExistsException;
import eu.europeana.cloud.service.mcs.exception.FileNotExistsException;
import eu.europeana.cloud.service.mcs.persistent.swift.ContentDAO;
import eu.europeana.cloud.service.mcs.persistent.swift.PutResult;
import eu.europeana.cloud.service.mcs.persistent.util.QueryTracer;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Repository;

import javax.annotation.PostConstruct;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

/**
 * Provides content DAO operations for Cassandra.
 *
 * @author krystian.
 */
@Repository
public class CassandraContentDAO implements ContentDAO {

    @Autowired
    @Qualifier("dbService")
    private CassandraConnectionProvider connectionProvider;

    private PreparedStatement insert;
    private PreparedStatement select;
    private PreparedStatement delete;

    private final StreamCompressor streamCompressor = new StreamCompressor();

    @PostConstruct
    private void prepareStatements() {
        Session s = connectionProvider.getSession();
        insert = s.prepare("INSERT INTO files_content (fileName, data) VALUES (?,?) IF NOT EXISTS");
        insert.setConsistencyLevel(connectionProvider
                .getConsistencyLevel());
        select = s.prepare("SELECT data FROM files_content WHERE fileName = ?;");
        select.setConsistencyLevel(connectionProvider
                .getConsistencyLevel());
        delete = s.prepare("DELETE FROM files_content WHERE fileName = ? IF EXISTS;");
        delete.setConsistencyLevel(connectionProvider
                .getConsistencyLevel());
    }

    /**
     * @inheritDoc
     */
    @Override
    public void copyContent(String sourceObjectId, String trgObjectId) throws FileNotExistsException, FileAlreadyExistsException, IOException {
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        try {
            getContent(sourceObjectId, -1, -1, os);
        } catch (FileNotExistsException e) {
            throw new FileNotExistsException(String.format("File %s not exists", sourceObjectId));
        }
        checkIfObjectNotExists(trgObjectId);
        putContent(trgObjectId, new ByteArrayInputStream(os.toByteArray()));
    }

    /**
     * @inheritDoc
     */
    @Override
    public void deleteContent(String fileName) throws FileNotExistsException {
        ResultSet rs = executeQueryWithLogger(delete.bind(fileName));
        if (!rs.wasApplied()) {
            throw new FileNotExistsException(String.format("File %s not exists", fileName));
        }
    }

    /**
     * @inheritDoc
     */
    @Override
    public void getContent(String fileName, long start, long end, OutputStream result) throws IOException,
            FileNotExistsException {
        ResultSet rs = executeQueryWithLogger(select.bind(fileName));

        Row row = rs.one();
        if (row == null) {
            throw new FileNotExistsException(String.format("File %s not exists", fileName));
        }
        ByteBuffer wrappedBytes = row.getBytes("data");
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        streamCompressor.decompress(unwrap(wrappedBytes), os);
        copySelectBytes(os,start,end, result);
    }

    /**
     * @inheritDoc
     */
    @Override
    public PutResult putContent(String fileName, InputStream data) throws IOException {
        CountingInputStream countingInputStream = new CountingInputStream(data);
        DigestInputStream md5DigestInputStream = prepareMd5DigestStream(countingInputStream);
        ByteBuffer wrappedBytes = ByteBuffer.wrap(streamCompressor.compress(md5DigestInputStream));
        executeQueryWithLogger(insert.bind(fileName, wrappedBytes));
        String md5 = BaseEncoding.base16().lowerCase().encode(md5DigestInputStream.getMessageDigest().digest());
        Long contentLength = countingInputStream.getCount();
        return new PutResult(md5, contentLength);
    }

    private void checkIfObjectNotExists(String trgObjectId) throws IOException, FileAlreadyExistsException {
        ResultSet rs = executeQueryWithLogger(select.bind(trgObjectId));
        Row row = rs.one();
        if (row != null) {
            throw new FileAlreadyExistsException(String.format("File %s already exists", trgObjectId));
        }
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
        IOUtils.copy(new ByteArrayInputStream(resultBytes),result);
    }

    private byte[] unwrap(ByteBuffer wrappedBytes) {
        return Bytes.getArray(wrappedBytes);
    }

    private byte[] getSelectedBytes(byte [] bytes, long start, long end) {
        byte[] outputBytes;
        final int from = start > -1 ? Ints.checkedCast(start) : 0;
        final int to = end > -1 ? Ints.checkedCast(end) + 1 : bytes.length;
        outputBytes = Arrays.copyOfRange(bytes, from, to);
        return outputBytes;
    }

    private DigestInputStream prepareMd5DigestStream(InputStream is) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            return new DigestInputStream(is, md);
        } catch (NoSuchAlgorithmException ex) {
            throw new AssertionError("Cannot get instance of MD5 but such algorithm should be provided", ex);
        }
    }
}
