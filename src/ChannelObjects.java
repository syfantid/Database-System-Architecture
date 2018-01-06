import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;


/**
 * This class is used as a data structure to store file channel, mapped byte buffer and file reference together as an object
 */
public class ChannelObjects {
    private FileChannel fileChannel;
    private MappedByteBuffer map;
    private RandomAccessFile randomAccessFile;

    /**
     * Constructor
     * @param fileChannel File channel
     * @param map Mapped Byte Buffer
     * @param randomAccessFile File object
     */
    public ChannelObjects(FileChannel fileChannel, MappedByteBuffer map, RandomAccessFile randomAccessFile) {
        this.fileChannel = fileChannel;
        this.map = map;
        this.randomAccessFile = randomAccessFile;
    }

    public FileChannel getFileChannel() {
        return fileChannel;
    }

    public MappedByteBuffer getMap() {
        return map;
    }

    public RandomAccessFile getRandomAccessFile() {
        return randomAccessFile;
    }
}
