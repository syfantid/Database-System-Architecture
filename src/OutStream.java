import java.io.*;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

class OutStream {

    /**
     * Creates file for system call function
     * @param filename
     * @return DataOutputStream
     * @throws FileNotFoundException
     */
    DataOutputStream systemCreate(String filename) throws FileNotFoundException {
        OutputStream os = new FileOutputStream( new File(filename) );
        return new DataOutputStream(os);
    }

    /**
     * Create file for buffered read and write stream with default buffer size
     * @param filename
     * @return DataOutputStream
     * @throws FileNotFoundException
     */
    DataOutputStream bufferedCreate(String filename) throws FileNotFoundException {
        OutputStream os = new FileOutputStream( new File(filename ) );
        BufferedOutputStream bos = new BufferedOutputStream( os );
        return new DataOutputStream( bos );
    }

    /**
     * Create file for buffered read and write stream with parameterized buffer size
     * @param filename
     * @param B Buffer Size in 32 bit integers
     * @return DataOutputStream
     * @throws FileNotFoundException
     */
    DataOutputStream bufferedCreate(String filename, int B) throws FileNotFoundException {
        OutputStream os = new FileOutputStream( new File(filename ) );
        /*Giving customized size of B as parameter in bytes*/
        BufferedOutputStream bos = new BufferedOutputStream( os, B * IOBenchmarking.elementSizeInBytes );
        return new DataOutputStream( bos );
    }

    /**
     * Create file for Object stream functions
     * @param filename
     * @return ObjectOutputStream
     * @throws IOException
     */
    ObjectOutputStream objectCreate(String filename) throws IOException {
        return new ObjectOutputStream(new FileOutputStream(filename));
    }

    /**
     * Create file for memory mapping file write
     * @param filename
     * @param N File size in 32-bit integers
     * @return
     * @throws IOException
     */
    ChannelObjects channelCreate(String filename, int N) throws IOException {
        RandomAccessFile memoryMappedFile = new RandomAccessFile(filename, "rw");
        FileChannel fileChannel = memoryMappedFile.getChannel();
        MappedByteBuffer map = fileChannel.map(FileChannel.MapMode.READ_WRITE,
                0, IOBenchmarking.elementSizeInBytes * N);
        return new ChannelObjects(fileChannel, map, memoryMappedFile);
    }

    /**
     * Write in the file using system calls and buffered streams
     * @param ds DataOutputStream
     * @param element elements to write in the file
     * @throws IOException
     */
    void write(DataOutputStream ds, int element) throws IOException {
        ds.writeInt(element);
    }

    /**
     * Write to file using object stream
     * @param oos ObjectOutputStream
     * @param buffer Buffer containing integers to write in the file
     * @throws IOException
     */
    void write(ObjectOutputStream oos, int[] buffer) throws IOException {
        oos.writeObject(buffer);
    }

    /**
     * Write to file using memory mapping
     * @param map MappedByteBuffer
     * @param buffer buffer with integers to write in the file
     * @throws IOException
     */
    void write(MappedByteBuffer map, int[] buffer) throws IOException {
        for (int element : buffer) {
            map.putInt(element);
        }
    }

    /**
     * Closes the file
     * @param ds OutputStream
     * @throws IOException
     */
    void close(OutputStream ds) throws IOException {
        ds.close();
    }

    /**
     * Closes the file and file channel for memory mapped mechanism
     * @param channelObjects
     * @throws IOException
     */
    void close(ChannelObjects channelObjects) throws IOException {
        channelObjects.getFileChannel().close();
        channelObjects.getRandomAccessFile().close();
    }
}
