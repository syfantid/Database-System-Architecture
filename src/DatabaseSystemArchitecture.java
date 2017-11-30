import java.io.*;
import java.nio.MappedByteBuffer;
import java.util.Random;

public class DatabaseSystemArchitecture {
    /* Number of open streams */
    private static int k = 5;
    /* Input and Output Streams; need to be initialized in main function */
    static OutStream[] out = new OutStream[k];
    static InStream[] in = new InStream[k];
    /* Variables for timing */
    static int elementSizeInBytes = 4;
    static long startReadTime;
    static long startWriteTime;
    static long endReadTime;
    static long endWriteTime;
    static long writeTime;
    static long readTime;
    static long totalTime;
    /* Variable for random integer generation */
    static Random rand = new Random();
    /* Number of records per file */
    static int N = 50000;
    /* Size of a buffer block in records */
    static int B = 100;
    // TODO: 30/11/2017 Before we had declared B as the number of buffers in memory; and M as the size of each memory
    // buffer but I think it was wrong.


    public static void main(String[] args) throws IOException, ClassNotFoundException {
        /* Initializing the input streams */
        for(int i=0; i<k; i++) {
            out[i] = new OutStream();
            in[i] = new InStream();
        }

        /* METHOD 1: SYSTEM FUNCTIONS */
        System.out.println("METHOD 1:");
        systemStreams();

        /* METHOD 2: BUFFERED READER AND WRITER */
        System.out.println("METHOD 2:");
        bufferedStreams();

        /* METHOD 3: BUFFER READ PER BLOCK */
        System.out.println("METHOD 3:");
        blockStreams();

        /* METHOD 4: MAPPING */
        System.out.println("METHOD 4:");
        mapStreams();
    }

    private static String createFilename(String filename, int i) {
        return filename + i + ".data";
    }

    private static void systemStreams() throws IOException {
        DataOutputStream[] ods = new DataOutputStream[k];
        DataInputStream[] ds = new DataInputStream[k];

        /* Write data to files */
        startWriteTime = System.currentTimeMillis();
        for(int i = 0; i < k; i++) {
            ods[i] = out[i].systemCreate(createFilename("testSystem",i));
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
                ds[i] = in[i].systemOpen(createFilename("testSystem",i));
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

    private static void bufferedStreams() throws IOException {
        DataOutputStream[] ods = new DataOutputStream[k];
        DataInputStream[] ds = new DataInputStream[k];

        /* Write data to files */
        startWriteTime = System.currentTimeMillis();
        for(int i = 0; i < k; i++) {
            ods[i] = out[i].bufferedCreate(createFilename("testBuffered",i));
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
                ds[i] = in[i].bufferedOpen(createFilename("testBuffered",i));
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

    private static void blockStreams() throws IOException, ClassNotFoundException {
        ObjectOutputStream[] oos = new ObjectOutputStream[k];
        ObjectInputStream[] ois = new ObjectInputStream[k];

        /* Write data to files */
        startWriteTime = System.currentTimeMillis();
        for(int i = 0; i < k; i++) {
            oos[i] = out[i].blockCreate(createFilename("testBlock",i));
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
                ois[i] = in[i].blockOpen(createFilename("testBlock",i));
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

    private static void mapStreams() throws IOException {
         /*Write data to file*/
        ChannelObjects[] channelObjects = new ChannelObjects[k];
        MappedByteBuffer map[] = new MappedByteBuffer[k];
        /* Write data to files */
        startWriteTime = System.currentTimeMillis();
        for(int i = 0; i < k; i++) {
            channelObjects[i] = out[i].channelCreate(createFilename("testMapping",i), N);
            map[i] = channelObjects[i].getMap();
        }

        int[] buffer = new int[B]; // A single buffer containing B elements
        // The number of times we will need to refill the buffer will be size of file / number of elements in each buffer
        int bufferRefills = (int)Math.ceil(N/B);
        int counter = 0; //to check if N integers are already written in the file

        /*refill the buffer array bufferRefills times using B elements and then write it to k files*/
        outerloop:
        for(int i = 0; i < bufferRefills; i++) {
            for(int j = 0; j < B; j++) {
                buffer[j] = rand.nextInt(10);
                counter++;
                if(counter >= N) {
                    break outerloop;
                }
            }

            for(int j = 0; j < k; j++) {
                /*System.out.println(j);*/
                out[j].write(map[j], buffer);
            }
        }

        /* Close all files */
        for(int i = 0; i < k; i++) {
            out[i].close(channelObjects[i]);
        }
        endWriteTime = System.currentTimeMillis();
        writeTime = endWriteTime - startWriteTime;
        System.out.println("Write Time: " + writeTime);

        /* Open files and read data */
        startReadTime = System.currentTimeMillis();
        for(int i = 0; i < k; i++) {
            try {
                channelObjects[i] = in[i].channelOpen(createFilename("testMapping",i),B);
                map[i] = channelObjects[i].getMap();
            } catch (FileNotFoundException e) {
                System.out.println("File not found!");
                return;
            }
        }

        for(int i = 0; i < k; i++) {
            while (in[i].eof(map[i])) {
                int[] buf = in[i].read(map[i], B);
                /* Uncomment this block if you want the result to be printed on screen */
//                for (int j = 0; j < buf.length; j++) {
//                    System.out.println(buf[j]);
//                }
//                System.out.println("**********");
            }
        }

        for(int i = 0; i < k; i++) {
            in[i].close(channelObjects[i]);
        }

        endReadTime = System.currentTimeMillis();
        readTime = endReadTime - startReadTime;
        System.out.println("Read Time: " + readTime);
        totalTime = readTime + writeTime;
        System.out.println("Total Time: " + totalTime);
    }
  }
