package eu.europeana.cloud.service.mcs.utils.storage_selector;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * InputStream with eager pre buffer initial number of bits.
 *
 * @author krystian.
 */
public class PreBufferedInputStream extends BufferedInputStream {

  private static final int DEFAULT_CHUNK_SIZE = 1024;
  private final int bufferSize;
  private byte[] buffer;
  private int bufferFill;
  private int position;

  /**
   * Creates {@link PreBufferedInputStream} and load data in to internal buffer.
   *
   * @param stream input stream
   * @param bufferSize size of buffer
   */
  public PreBufferedInputStream(InputStream stream, int bufferSize) {
    super(stream, DEFAULT_CHUNK_SIZE);
    this.bufferSize = bufferSize;
    this.buffer = new byte[bufferSize];
    try {
      fillBuffer();
    } catch (IOException e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  public static PreBufferedInputStream wrap(final byte[] data,
      final int preloadChunkSize) {
    return new PreBufferedInputStream(new ByteArrayInputStream(data), preloadChunkSize);
  }

  /**
   * Get internal buffer size.
   *
   * @return buffer size
   */
  public int getBufferSize() {
    return bufferSize;
  }

  private void fillBuffer() throws IOException {
    ensureIsNotClosed();
    for (int i = 0; i < bufferSize; i += DEFAULT_CHUNK_SIZE) {
      int bufferLength = Math.min(bufferSize - bufferFill, DEFAULT_CHUNK_SIZE);
      byte[] bytes = new byte[bufferLength];
      final int length = super.read(bytes, 0, bufferLength);
      if (length != -1) {
        System.arraycopy(bytes, 0, this.buffer, bufferFill, length);
        bufferFill += length;
      }
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public synchronized int read() throws IOException {
    ensureIsNotClosed();
    if (position < bufferFill) {
      return buffer[position++] & 0xff;
    }
    return super.read();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public synchronized int read(final byte[] b, final int offset, final int length) throws IOException {
    ensureIsNotClosed();
    if (b == null) {
      throw new NullPointerException();
    } else if (offset < 0 || length < 0 || length > b.length - offset) {
      throw new IndexOutOfBoundsException();
    } else if (length == 0) {
      return 0;
    }
    int result;
    result = readFromBufferAndStream(b, offset, length);
    return result;
  }

  private int readFromBufferAndStream(final byte[] b, int off, int len) throws IOException {
    int offset = off;
    int length = len;
    int available = bufferFill - position;
    if (available > 0) { //read pre-loaded bytes from internal buffer
      if (length < available) {
        available = length;
      }
      System.arraycopy(buffer, position, b, offset, available);
      position += available;
      offset += available;
      length -= available;
    }
    if (length > 0) { //read from stream
      length = super.read(b, offset, length);
      if (length == -1) {
        return available == 0 ? -1 : available;
      } else {
        return available + length;
      }
    }
    return available;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public synchronized int available() throws IOException {
    ensureIsNotClosed();
    int inBufferAvailable = bufferFill - position;
    int avail = super.available();
    return inBufferAvailable > (Integer.MAX_VALUE - avail)
        ? Integer.MAX_VALUE
        : (inBufferAvailable + avail);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean markSupported() {
    return true;
  }

  /**
   * {@inheritDoc}
   *
   * </br>
   * Marking is available only in range of internal buffer {@link PreBufferedInputStream#getBufferSize()}.
   */
  @Override
  public synchronized void mark(int readLimit) {
    if (readLimit > bufferSize) {
      throw new UnsupportedOperationException("Marking outside buffer is not supported!");
    }
    marklimit = readLimit;
    markpos = position;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public synchronized void reset() throws IOException {
    ensureIsNotClosed();
    if (markpos < 0) {
      throw new IOException("Resetting to invalid mark");
    }
    position = markpos;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public synchronized void close() throws IOException {
    if (in == null && buffer == null) {
      return;
    }
    buffer = null;
    in.close();
    in = null;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public synchronized long skip(final long length) throws IOException {
    ensureIsNotClosed();
    long skip = length;
    if (skip <= 0) {
      return 0;
    }
    long actualSkip = (long) bufferFill - position;
    if (actualSkip > 0) {
      if (skip < actualSkip) {
        actualSkip = skip;
      }
      position += actualSkip;
      skip -= actualSkip;
    }
    if (skip > 0) {
      long currentSkip;
      do {
        currentSkip = super.skip(skip);
        actualSkip += currentSkip;
        skip -= currentSkip;
      } while (currentSkip > 0 && skip > 0);

    }
    return actualSkip;
  }

  private void ensureIsNotClosed() throws IOException {
    if (in == null) {
      throw new IOException("Stream closed");
    }
  }
}
