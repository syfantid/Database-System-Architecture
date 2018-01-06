import java.io.*;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**
 * This class is responsible for all input operations
 */
public class InStream {

    /**
     * Opens an unbuffered stream for reading
     * @param filename The filename of the file to be read
     * @return The DataInputStream object for file handling
     * @throws FileNotFoundException If file not present in working directory
     */
    public DataInputStream systemOpen(String filename) throws FileNotFoundException {
        InputStream is = null;
        is = new FileInputStream( new File(filename) );
        DataInputStream ds = new DataInputStream(is);
        return ds;
    }

    /**
     * Opens a buffered stream with default buffer size for reading
     * @param filename The filename of the file to be read
     * @return The DataInputStream object for file handling
     * @throws FileNotFoundException If file not present in working directory
     */
    public DataInputStream bufferedOpen(String filename) throws FileNotFoundException {
        InputStream is = new FileInputStream( new File(filename ) );
        BufferedInputStream bis = new BufferedInputStream( is );
        DataInputStream ds = new DataInputStream( bis );
        return ds;
    }

    /**
     * Opens a buffered stream with user-specified buffer size for reading
     * @param filename The filename of the file to be read
     * @param B The size of the buffer
     * @return The DataInputStream object for file handling
     * @throws FileNotFoundException If file not present in working directory
     */
    public DataInputStream bufferedOpen(String filename, int B) throws FileNotFoundException {
        InputStream is = new FileInputStream( new File(filename ) );
        /*Giving customized size of B as parameter in bytes*/
        BufferedInputStream bis = new BufferedInputStream( is , B * IOBenchmarking.elementSizeInBytes );
        DataInputStream ds = new DataInputStream( bis );
        return ds;
    }

    /**
     * Opens an object stream for reading object files
     * @param filename The filename of the file to be read
     * @return The ObjectInputStream object for file handling
     * @throws IOException If file not present in working directory
     */
    public ObjectInputStream objectOpen(String filename) throws IOException {
        ObjectInputStream in = new ObjectInputStream(new FileInputStream(filename));
        return in;
    }

    /**
     * Opens a channel and maps a file to virtual memory
     * @param filename The filename of the file to be read
     * @param N File size in integers
     * @return A ChannelObjects object for file handling
     * @throws IOException If file not present in working directory
     */
    public ChannelObjects channelOpen(String filename, int N) throws IOException {
        RandomAccessFile memoryMappedFile = new RandomAccessFile(filename, "r");
        FileChannel fileChannel = memoryMappedFile.getChannel();
        MappedByteBuffer map = fileChannel.map(FileChannel.MapMode.READ_ONLY,
                0, IOBenchmarking.elementSizeInBytes * N);
        return new ChannelObjects(fileChannel, map, memoryMappedFile);
    }

    /**
     * Reads a single integer to memory or buffer
     * @param ds The stream to read from
     * @return The integer read
     * @throws IOException
     */
    public int read(DataInputStream ds) throws IOException {
        return ds.readInt();
    }

    /**
     * Reads a buffer object from the stream
     * @param is The stream to read from
     * @return The buffer block read
     * @throws IOException
     * @throws ClassNotFoundException If object read is not of an array format
     */
    public int[] read(ObjectInputStream is) throws IOException, ClassNotFoundException {
        int[] buffer;
        buffer = (int[]) is.readObject();
        return buffer;
    }

    /**
     * Reads B integers from mapped file
     * @param map The mapped buffer
     * @param B The number of integers to be read
     * @return The buffer block
     * @throws IOException
     */
    public int[] read( MappedByteBuffer map, int B) throws IOException {
        int[] buffer = new int [B];
        for (int i = 0; i < B; i++) {
            buffer[i] = map.getInt();
        }
        return buffer;
    }

    /**
     * Identifies the end of file
     * @param ds The stream to examine
     * @return True if end of file, false otherwise
     * @throws IOException
     */
    public boolean eof(DataInputStream ds) throws IOException {
        return ds.available() <= 0;
    }

    /**
     * Identifies the end of file, whether there are more objects to be read
     * @param ois The stream to examine
     * @return True if end of file, false otherwise
     * @throws IOException
     */
    public boolean eof(ObjectInputStream ois) throws IOException {
        return ois.available() <= 0;
    }

    /**
     * Identifies the end of file
     * @param mbb The mapped buffer to be examined
     * @return True if end of file, false otherwise
     */
    public boolean eof(MappedByteBuffer mbb) {
        return !mbb.hasRemaining();
    }

    /**
     * Closes the stream
     * @param ds The stream to be closed
     * @throws IOException
     */
    public void close(DataInputStream ds) throws IOException {
        ds.close();
    }

    /**
     * Closes the object stream
     * @param ois The stream to be closed
     * @throws IOException
     */
    public void close(ObjectInputStream ois) throws IOException {
        ois.close();
    }

    /**
     * Closes both the channel and the random access file; the mapping is destroyed only after garbage collector
     * collects the mapped buffer
     * @param channelObjects The objects to be closed
     * @throws IOException
     */
    public void close(ChannelObjects channelObjects) throws IOException {
        channelObjects.getFileChannel().close();
        channelObjects.getRandomAccessFile().close();
        //System.gc();
    }
}
