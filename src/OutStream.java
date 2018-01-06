import java.io.*;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

class OutStream {

    DataOutputStream systemCreate(String filename) throws FileNotFoundException {
        OutputStream os = new FileOutputStream( new File(filename) );
        return new DataOutputStream(os);
    }

    DataOutputStream bufferedCreate(String filename) throws FileNotFoundException {
        OutputStream os = new FileOutputStream( new File(filename ) );
        BufferedOutputStream bos = new BufferedOutputStream( os );
        return new DataOutputStream( bos );
    }

    DataOutputStream bufferedCreate(String filename, int B) throws FileNotFoundException {
        OutputStream os = new FileOutputStream( new File(filename ) );
        /*Giving customized size of B as parameter in bytes*/
        BufferedOutputStream bos = new BufferedOutputStream( os, B * IOBenchmarking.elementSizeInBytes );
        return new DataOutputStream( bos );
    }


    ObjectOutputStream objectCreate(String filename) throws IOException {
        return new ObjectOutputStream(new FileOutputStream(filename));
    }

    /**
     *
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

    void write(DataOutputStream ds, int element) throws IOException {
        ds.writeInt(element);
    }

    void write(ObjectOutputStream oos, int[] buffer) throws IOException {
        oos.writeObject(buffer);
    }

    void write(MappedByteBuffer map, int[] buffer) throws IOException {
        for (int element : buffer) {
            map.putInt(element);
        }
    }

    void close(OutputStream ds) throws IOException {
        ds.close();
    }

    void close(ChannelObjects channelObjects) throws IOException {
        channelObjects.getFileChannel().close();
        channelObjects.getRandomAccessFile().close();
        //System.gc();
    }
}
