package jepsen.store.format;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.zip.CRC32;

// This class provides an OutputStream linked to a FileChannel at a particular
// offset; each write to this stream is written to the corresponding file. Also
// tracks the count and CRC32 of all streamed bytes.
public class FileOffsetOutputStream extends OutputStream implements AutoCloseable {
  public final FileChannel file;
  public final long offset;
  public final ByteBuffer singleByteBuffer;
  public long currentOffset;
  public final CRC32 checksum;

  public FileOffsetOutputStream(FileChannel file, long offset, CRC32 checksum) {
    super();
    this.file = file;
    this.offset = offset;
    this.currentOffset = offset;
    this.singleByteBuffer = ByteBuffer.allocate(1);
    this.checksum = checksum;
  }

  // Returns how many bytes have been written to this stream
  public long bytesWritten() {
    return currentOffset - offset;
  }

  // Returns current CRC32
  public CRC32 checksum() {
    return checksum;
  }

  public void close() {
  }

  public void flush() throws IOException {
    file.force(false);
  }

  public void write(int b) throws IOException {
    //System.out.printf("Wrote %d to offset %d\n", b, currentOffset);
    // Copy byte into our buffer
    singleByteBuffer.put(0, (byte) b);
    // Write buffer and advance
    singleByteBuffer.rewind();
    final int written = file.write(singleByteBuffer, currentOffset);
    assert written == 1;
    currentOffset += written;
    checksum.update(b);
  }

  public void write(byte[] bs) throws IOException {
    //System.out.printf("Wrote fast %d", bs.length);
    final ByteBuffer buf = ByteBuffer.wrap(bs);
    final int written = file.write(buf, currentOffset);
    assert written == buf.limit();
    currentOffset += written;
    checksum.update(bs);
  }

  public void write(byte[] bs, int offset, int len) throws IOException {
    //System.out.printf("Wrote fast %d", len);
    final ByteBuffer buf = ByteBuffer.wrap(bs, offset, len);
    final int written = file.write(buf, currentOffset);
    assert written == buf.limit();
    currentOffset += written;
    checksum.update(bs, offset, len);
  }
}
