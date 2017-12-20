import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Random;

public class MergesortBenchmarking {
    static int N = 100; // Size of initial file in integers
    static int B = 10; // Integers to read at once (as block) for the mapped I/Os
    static int M = 20; // Size of memory in integers
    static int d = 3; // Streams to be open at once in merge sort after initial pass
    // d should be less than 30 and less than M


    public static void main(String[] args) throws Exception {
        String initialFile = "intial.data";
        createInitialFile(initialFile, N, B);

        MultiwayMergeSort merge = new MultiwayMergeSort(M,d,N);
        int sortedFile = merge.mergeSort(initialFile);
        System.out.println("Sorted file number: " + sortedFile);

        InStream in = new InStream();
        ChannelObjects channelObject;
            try {
//                channelObject = in.channelOpen(IOBenchmarking.createFilename("file",sortedFile),N);
                channelObject = in.channelOpen("file6.data",100);
            } catch (FileNotFoundException e) {
                System.out.println("File not found!");
                return;
            }

            while (!in.eof(channelObject.getMap())) {
                int[] buf = in.read(channelObject.getMap(), B);
                /* Uncomment this block if you want the result to be printed on screen */
                for (int j = 0; j < buf.length; j++) {
                    System.out.println(buf[j]);
                }
                System.out.println("**********");
            }

            in.close(channelObject);

    }

    public static void createInitialFile(String filename, int N, int B) throws IOException {
        OutStream out = new OutStream();
        ChannelObjects channelObject;
        /* Write data to files */
        channelObject = out.channelCreate(filename, N);

        int[] buffer = new int[B]; // A single buffer containing B elements
        // The number of times we will need to refill the buffer will be size of file / number of elements in each buffer
        int bufferRefills = (int)Math.ceil(N/B);
        int counter = 0; //to check if N integers are already written in the file
        Random rand = new Random();

        /*refill the buffer array bufferRefills times using B elements and then write it to file*/
        for(int i = 0; i < bufferRefills; i++) {
            innerloop:
            for(int j = 0; j < B; j++) {
                buffer[j] = rand.nextInt(50) + 1;
                counter++;
                if(counter >= N) {
                    break innerloop;
                }
            }
            out.write(channelObject.getMap(), buffer);
        }

        /* Close file */
        out.close(channelObject);
    }
}
