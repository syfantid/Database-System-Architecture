import java.io.*;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

public class OutStream {

    public DataOutputStream systemCreate(String filename) throws FileNotFoundException {
        OutputStream os = new FileOutputStream( new File(filename) );
        DataOutputStream ds = new DataOutputStream(os);
        return ds;
    }

    public DataOutputStream bufferedCreate(String filename) throws FileNotFoundException {
        OutputStream os = new FileOutputStream( new File(filename ) );
        BufferedOutputStream bos = new BufferedOutputStream( os );
        DataOutputStream ds = new DataOutputStream( bos );
        return ds;
    }

    public ObjectOutputStream blockCreate(String filename) throws IOException {
        ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(filename));
        return out;
    }

    public ChannelObjects channelCreate(String filename, int M) throws IOException {
        RandomAccessFile memoryMappedFile = new RandomAccessFile(filename, "rw");
        FileChannel fileChannel = memoryMappedFile.getChannel();
        MappedByteBuffer map = fileChannel.map(FileChannel.MapMode.READ_WRITE,
                0,DatabaseSystemArchitecture.elementSizeInBytes * M);
        return new ChannelObjects(fileChannel, map, memoryMappedFile);
    }

    public void write(DataOutputStream ds, int element) throws IOException {
        ds.writeInt(element);
}

    public void write(ObjectOutputStream oos, int[] buffer) throws IOException {
        oos.writeObject(buffer);
    }

    public void write(MappedByteBuffer map, int[] buffer) throws IOException {
        for (int element : buffer) {
            map.putInt(element);
        }
    }

    public void close(OutputStream ds) throws IOException {
        ds.close();
    }

    public void close(ChannelObjects channelObjects) throws IOException {
        channelObjects.getFileChannel().close();
        channelObjects.getRandomAccessFile().close();
    }
}
