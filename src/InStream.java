import java.io.*;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

public class InStream {

    public DataInputStream systemOpen(String filename) throws FileNotFoundException {
        InputStream is = null;
        is = new FileInputStream( new File(filename) );
        DataInputStream ds = new DataInputStream(is);
        return ds;
    }

    public DataInputStream bufferedOpen(String filename) throws FileNotFoundException {
        InputStream is = new FileInputStream( new File(filename ) );
        BufferedInputStream bis = new BufferedInputStream( is );
        DataInputStream ds = new DataInputStream( bis );
        return ds;
    }
    /*Open file with a specific buffer size*/
    public DataInputStream bufferedOpen(String filename, int B) throws FileNotFoundException {
        InputStream is = new FileInputStream( new File(filename ) );
        /*Giving customized size of B as parameter in bytes*/
        BufferedInputStream bis = new BufferedInputStream( is , B * IOBenchmarking.elementSizeInBytes );
        DataInputStream ds = new DataInputStream( bis );
        return ds;
    }

    public ObjectInputStream objectOpen(String filename) throws IOException {
        ObjectInputStream in = new ObjectInputStream(new FileInputStream(filename));
        return in;
    }

    /**
     *
     * @param filename The file to be opened
     * @param N File size in integers
     * @return
     * @throws IOException
     */
    public ChannelObjects channelOpen(String filename, int N) throws IOException {
        RandomAccessFile memoryMappedFile = new RandomAccessFile(filename, "r");
        FileChannel fileChannel = memoryMappedFile.getChannel();
        MappedByteBuffer map = fileChannel.map(FileChannel.MapMode.READ_ONLY,
                0, IOBenchmarking.elementSizeInBytes * N);
        return new ChannelObjects(fileChannel, map, memoryMappedFile);
    }

    public int read(DataInputStream ds) throws IOException {
        return ds.readInt();
    }

    public int[] read(ObjectInputStream is) throws IOException, ClassNotFoundException {
        int[] buffer;
        buffer = (int[]) is.readObject();
        return buffer;
    }

    public int[] read( MappedByteBuffer map, int B) throws IOException {
        int[] buffer = new int [B];
        for (int i = 0; i < B; i++) {
            buffer[i] = map.getInt();
        }
        return buffer;
    }

    public boolean eof(DataInputStream ds) throws IOException {
        return ds.available() <= 0;
    }

    public boolean eof(ObjectInputStream ois) throws IOException {
        return ois.available() <= 0;
    }

    public boolean eof(MappedByteBuffer mbb) {
        return !mbb.hasRemaining();
    }

    public void close(DataInputStream ds) throws IOException {
        ds.close();
    }

    public void close(ObjectInputStream ois) throws IOException {
        ois.close();
    }

    public void close(ChannelObjects channelObjects) throws IOException {
        channelObjects.getFileChannel().close();
        channelObjects.getRandomAccessFile().close();
        //System.gc();
    }
}
