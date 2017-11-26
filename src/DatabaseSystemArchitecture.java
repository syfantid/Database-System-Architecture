import java.io.*;
import java.nio.MappedByteBuffer;
import java.util.ArrayList;
import java.util.Random;

public class DatabaseSystemArchitecture {
    /* Number of open streams */
    public static int k = 10;
    /* Input and Output Streams; need to be initialized in main function */
    public static OutStream[] out = new OutStream[k];
    public static InStream[] in = new InStream[k];
    /* Variables for timing */
    public static int elementSizeInBytes = 4;
    public static long startReadTime;
    public static long startWriteTime;
    public static long endReadTime;
    public static long endWriteTime;
    public static long writeTime;
    public static long readTime;
    public static long totalTime;
    /* Variable for random integer generation */
    public static Random rand = new Random();
    /* Number of records per file */
    public static int N = 50000;
    /* Number of buffer blocks */
    public static int B = 20;


    public static void main(String[] args) throws IOException, ClassNotFoundException {
        /* Initializing the input streams */
        for(int i=0; i<k; i++) {
            out[i] = new OutStream();
            in[i] = new InStream();
        }

        /* METHOD 1: SYSTEM FUNCTIONS */
        System.out.println("METHOD 1:");
        systemStreams("testSystem");

        /* METHOD 2: BUFFERED READER AND WRITER */
        System.out.println("METHOD 2:");
        bufferedStreams("testBuffered");

        /* METHOD 3: BUFFER READ PER BLOCK */
        System.out.println("METHOD 3:");
        blockStreams("testBlock");

        /* METHOD 4: MAPPING */
        /*System.out.println("METHOD 4:");
        mapStreams("testMapping");*/
    }

    static String createFilename(String filename, int i) {
        return filename + i + ".data";
    }

    static void systemStreams(String filename) throws IOException {
        DataOutputStream[] ods = new DataOutputStream[k];
        DataInputStream[] ds = new DataInputStream[k];

        /* Write data to files */
        startWriteTime = System.currentTimeMillis();
        for(int i = 0; i < k; i++) {
            ods[i] = out[i].systemCreate(createFilename(filename,i));
        }
        /* For each number (i), write the number to j files */
        for(int i = 0; i < N; i++) {
            for(int j = 0; j < k; j++) {
                out[j].write(ods[j], rand.nextInt(10));
            }
        }
        /* Close all files */
        for(int i = 0; i < k; i++) {
            out[i].close(ods[i]);
        }

        endWriteTime = System.currentTimeMillis();
        writeTime = endWriteTime - startWriteTime;
        System.out.println("Write Time: " + writeTime);

        /* Open files simultaneously */
        startReadTime = System.currentTimeMillis();
        for(int i = 0; i < k; i++) {
            try {
                ds[i] = in[i].systemOpen(createFilename(filename,i));
            } catch (FileNotFoundException e) {
                System.out.println("File not found!");
                return;
            }
        }
        /* Read data from files */
        for(int i = 0; i < k; i++) {
            while (!in[i].eof(ds[i])) {
                in[i].read(ds[i]);
                /* Uncomment this line if you want the contents of the file to be printed on screen */
               // System.out.println(in[i].read(ds[i]));
            }
        }
        /* Close all files */
        for(int i = 0; i < k; i++) {
            in[i].close(ds[i]);
        }

        endReadTime = System.currentTimeMillis();
        readTime = endReadTime - startReadTime;
        System.out.println("Read Time: " + readTime);
        totalTime = readTime + writeTime;
        System.out.println("Total Time: " + totalTime);
    }

    static void bufferedStreams(String filename) throws IOException {
        DataOutputStream[] ods = new DataOutputStream[k];
        DataInputStream[] ds = new DataInputStream[k];

        /* Write data to files */
        startWriteTime = System.currentTimeMillis();
        for(int i = 0; i < k; i++) {
            ods[i] = out[i].bufferedCreate(createFilename(filename,i));
        }
        /* For each number (i), write the number to j files */
        for(int i = 0; i < N; i++) {
            for(int j = 0; j < k; j++) {
                out[j].write(ods[j], rand.nextInt());
            }
        }
        /* Close all files */
        for(int i = 0; i < k; i++) {
            out[i].close(ods[i]);
        }

        /*Open file and read data*/
        endWriteTime = System.currentTimeMillis();
        writeTime = endWriteTime - startWriteTime;
        System.out.println("Write Time: " + writeTime);

        /* Open files simultaneously */
        startReadTime = System.currentTimeMillis();
        for(int i = 0; i < k; i++) {
            try {
                ds[i] = in[i].bufferedOpen(createFilename(filename,i));
            } catch (FileNotFoundException e) {
                System.out.println("File not found!");
                return;
            }
        }

        /* Read data from files */
        for(int i = 0; i < k; i++) {
            while (!in[i].eof(ds[i])) {
                in[i].read(ds[i]);
            /* Uncomment this line if you want the contents of the file to be printed on screen */
            /*System.out.println(in[i].read(ds[i]));*/
            }
        }
        /* Close all files */
        for(int i = 0; i < k; i++) {
            in[i].close(ds[i]);
        }

        endReadTime = System.currentTimeMillis();
        readTime = endReadTime - startReadTime;
        System.out.println("Read Time: " + readTime);
        totalTime = readTime + writeTime;
        System.out.println("Total Time: " + totalTime);
    }

    static void blockStreams(String filename) throws IOException, ClassNotFoundException {
        ObjectOutputStream[] oos = new ObjectOutputStream[k];
        ObjectInputStream[] ois = new ObjectInputStream[k];

        /* Write data to files */
        startWriteTime = System.currentTimeMillis();
        for(int i = 0; i < k; i++) {
            oos[i] = out[i].blockCreate(createFilename(filename,i));
        }

        int[] buffer = new int[B];
        int bufferRefills = (int)Math.ceil(N/B); //size of each buffer will be size of file / number of elements in each buffer
        int counter = 0; //to check if N integers are already written in the file

        /*refill the buffer array bufferRefills times using B elements and then write it to k files*/
        outerloop:
        for(int i = 0; i < bufferRefills; i++) {
            for(int j = 0; j < B; j++) {
                buffer[j] = rand.nextInt();
                counter++;
                if(counter >= N) {
                    break outerloop;
                }
            }
            for(int j = 0; j < k; j++) {
                out[j].write(oos[j], buffer);
            }
        }

        /* Close all files */
        for(int i = 0; i < k; i++) {
            out[i].close(oos[i]);
        }
        endWriteTime = System.currentTimeMillis();
        writeTime = endWriteTime - startWriteTime;
        System.out.println("Write Time: " + writeTime);

        /*Open file and read data */
        startReadTime = System.currentTimeMillis();
        for(int i = 0; i < k; i++) {
            try {
                ois[i] = in[i].blockOpen(createFilename(filename,i));
            } catch (FileNotFoundException e) {
                System.out.println("File not found!");
                return;
            }
        }
        /*read all files one buffer at a time*/
        for(int i = 0; i < k; i++) {
            while(true){
                try {
                    buffer = in[i].read(ois[i]);
                    /*Uncomment if you want to print the results*/
                    /*
                    for(int element:buffer) {
                        System.out.println(element);
                     }
                    System.out.println("**************");
                   */
                } catch (EOFException e) {
                    in[i].close(ois[i]);
                    break;
                }
            }
        }
        endReadTime = System.currentTimeMillis();
        readTime = endReadTime - startReadTime;
        System.out.println("Read Time: " + readTime);
        totalTime = readTime + writeTime;
        System.out.println("Total Time: " + totalTime);
    }

//    static void mapStreams(String filename) throws IOException {
//         Write data to file
//        int[] buffer = {1,2,3};
//        int[] buffer2 = {4,5,6};
//        int[] buffer3 = {7,8,9};
//        int[] buffer4 = {10,11,12};
//        ChannelObjects channelObjects = out.channelCreate(filename, M);
//        MappedByteBuffer map = channelObjects.getMap();
//        out.write(map, buffer);
//        out.write(map, buffer2);
//        out.write(map, buffer3);
//        out.write(map, buffer4);
//        out.close(channelObjects);
//
//         Open file and read data
//        channelObjects = in.channelOpen(filename, M);
//        map = channelObjects.getMap();
//        while(in.eof(map)) {
//            int[] buf = in.read(map, B);
//            for(int i = 0 ; i < buf.length ; i++) {
//                System.out.println(buf[i]);
//            }
//            System.out.println("**********");
//        }
//        in.close(channelObjects);
//    }
  }
