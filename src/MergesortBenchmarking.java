import javafx.scene.shape.Path;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Random;

public class MergesortBenchmarking {
    static int N; // Size of initial file in integers
    static int B = 10; // Integers to read at once (as block) for the mapped I/Os ONLY to create a random file in the start
    static int M; // Size of memory in integers
    static int d; // Streams to be open at once in merge sort after initial pass
    // d should be less than 30 and less than M-1

    public static void main(String[] args) throws Exception {

        if(args.length < 3) {
            System.err.println("Please provide all the necessary arguments.\n" +
                    "Argument order: N M d");
            return;
        }

        N = Integer.valueOf(args[0]);
        M = Integer.valueOf(args[1]);
        d = Integer.valueOf(args[2]);
        if(d >= M) {
            System.err.println("Please provide sufficient memory for merging d streams.");
            return;
        }
        if(d > 30) {
            System.err.println("The number of simultaneously opened streams, d, cannot exceed 30.");
            return;
        }

        String initialFile = "initial.data";
        createInitialFile(initialFile, N, B);

        MultiwayMergeSort merge = new MultiwayMergeSort(M,d,N);

        int sortedFile = merge.mergeSort(initialFile);
        System.out.println("Sorted file name: " + IOBenchmarking.createFilename("file", sortedFile));

        InStream in = new InStream();
        ChannelObjects channelObject;
            try {
//                channelObject = in.channelOpen(IOBenchmarking.createFilename("file",sortedFile),N);
                channelObject = in.channelOpen(IOBenchmarking.createFilename("file",sortedFile), N);
            } catch (FileNotFoundException e) {
                System.out.println("File not found!");
                return;
            }

        /* Uncomment this block if you want the result to be printed on screen */
//            int read = 0;
//            while (!in.eof(channelObject.getMap())) {
//                int[] buf = in.read(channelObject.getMap(), Math.min(B,N-read));
//                read+=B;
//                for (int j = 0; j < buf.length; j++) {
//                    System.out.println(buf[j]);
//                }
//                System.out.println("**********");
//            }

            in.close(channelObject);
//            deleteTemporaryFiles(sortedFile);



    }

    public static void deleteTemporaryFiles(int limit) throws IOException {
        for(int i = 0; i < limit; i++) {
            Files.deleteIfExists(Paths.get(IOBenchmarking.createFilename("file",i)));
        }
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
                buffer[j] = rand.nextInt(500000000) + 1;
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
