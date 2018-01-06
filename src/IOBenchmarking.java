import java.io.*;
import java.nio.MappedByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Random;

public class IOBenchmarking {
    /* Number of open streams */
    private static int k;
    /* Input and Output Streams; need to be initialized in main function */
    static OutStream[] out;
    static InStream[] in;
    /* Variables for timing */
    static final int elementSizeInBytes = 4;
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
    static int N;
    /* Size of a buffer block in records */
    static int B;

    /**
     * The main benchmarking method for the I/O operations
     * @param args The parameters to be passed for benchmarking, N, B and k
     * @throws IOException Exceptions thrown from various methods
     * @throws ClassNotFoundException Exceptions thrown from typecasting problems
     */
    public static void main(String[] args) throws IOException, ClassNotFoundException {

        if(args.length < 3) {
            System.err.println("Please provide all the necessary arguments.\n" +
                    "Argument order: N B k");
            return;
        }

        N = Integer.valueOf(args[0]);
        B = Integer.valueOf(args[1]);
        k = Integer.valueOf(args[2]);
        if(k > 30) {
            System.err.println("The number of simultaneously opened streams, k, cannot exceed 30.");
            return;
        }

        /* Input and Output Streams; need to be initialized in main function */
        out = new OutStream[k];
        in = new InStream[k];

        /* Initializing the input streams */
        for(int i=0; i<k; i++) {
            out[i] = new OutStream();
            in[i] = new InStream();
        }

        /* METHOD 1: SYSTEM FUNCTIONS */
        System.out.println("System Calls:");
        systemStreams();

        /* METHOD 2: BUFFERED READER AND WRITER */
        System.out.println("Default Buffered Streams:");
        bufferedStreams();

        /* METHOD 3a: BUFFER READ PER OBJECT (objectStream) */
        System.out.println("Object Streams:");
        objectStreams();

        /* METHOD 3b: BUFFER READ PER BLOCK (Buffered Parameterized Stream) */
        System.out.println("Parametrized Buffered Streams:");
        parameterizedBufferedStreams();

        /* METHOD 4: MAPPING */
       System.out.println("Memory Mapping:");
       mapStreams();
    }

    /**
     * Creates a filename for each file
     * @param filename The prefix of the filename
     * @param i The number of the file
     * @return The filename of the file to be written or read
     */
    public static String createFilename(String filename, int i) {
        return filename + i + ".data";
    }

    /**
     * Deletes a file
     * @param prefix The prefix of the file to be deleted
     * @param number The number of the file to be deleted
     * @throws IOException
     */
    public static void deleteFiles(String prefix, int number) throws IOException {
        Path fileToDeletePath = Paths.get(createFilename(prefix,number));
        Files.delete(fileToDeletePath);
    }

    /**
     * Benchmarks the system calls
     * @throws IOException
     */
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
            deleteFiles("testSystem",i);
        }

        endReadTime = System.currentTimeMillis();
        readTime = endReadTime - startReadTime;
        System.out.println("Read Time: " + readTime);
        totalTime = readTime + writeTime;
        System.out.println("Total Time: " + totalTime);
    }

    /**
     * Benchmarks the default buffered streams
     * @throws IOException
     */
    private static void bufferedStreams() throws IOException {
        DataOutputStream[] ods = new DataOutputStream[k];
        DataInputStream[] ds = new DataInputStream[k];

        /* Write data to files */
        startWriteTime = System.currentTimeMillis();
        for(int i = 0; i < k; i++) {
            ods[i] = out[i].bufferedCreate(createFilename("testBuffered",i));
        }
        /* For each number (i), write the number to j files through buffer */
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
            deleteFiles("testBuffered",i);
        }

        endReadTime = System.currentTimeMillis();
        readTime = endReadTime - startReadTime;
        System.out.println("Read Time: " + readTime);
        totalTime = readTime + writeTime;
        System.out.println("Total Time: " + totalTime);
    }

    /**
     * Benchmarks the object streams
     * @throws IOException
     * @throws ClassNotFoundException
     */
    private static void objectStreams() throws IOException, ClassNotFoundException {
        ObjectOutputStream[] oos = new ObjectOutputStream[k];
        ObjectInputStream[] ois = new ObjectInputStream[k];

        /* Write data to files */
        startWriteTime = System.currentTimeMillis();
        for(int i = 0; i < k; i++) {
            oos[i] = out[i].objectCreate(createFilename("testObject",i));
        }

        int[] buffer = new int[B];
        int bufferRefills = (int)Math.ceil(N/B); //size of each buffer will be size of file / number of elements in each buffer
        int counter = 0; //to check if N integers are already written in the file

        /* Refill the buffer array bufferRefills times using B elements and then write it to k files */
        for(int i = 0; i < bufferRefills; i++) {
            innerloop:
            for(int j = 0; j < B; j++) {
                buffer[j] = rand.nextInt();
                counter++;
                if(counter >= N) {
                    break innerloop;
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
                ois[i] = in[i].objectOpen(createFilename("testObject",i));
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
                    deleteFiles("testObject",i);
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

    /**
     * Benchmarks the parametrized buffered streams
     * @throws IOException
     */
    private static void parameterizedBufferedStreams() throws IOException {
        DataOutputStream[] ods = new DataOutputStream[k];
        DataInputStream[] ds = new DataInputStream[k];

        /* Write data to files */
        startWriteTime = System.currentTimeMillis();
        for(int i = 0; i < k; i++) {
            ods[i] = out[i].bufferedCreate(createFilename("testBuffered",i), B);
        }
        /* For each number (i), write the number to j files through buffer */
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
                ds[i] = in[i].bufferedOpen(createFilename("testBuffered",i), B);
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
            deleteFiles("testBuffered",i);
        }

        endReadTime = System.currentTimeMillis();
        readTime = endReadTime - startReadTime;
        System.out.println("Read Time: " + readTime);
        totalTime = readTime + writeTime;
        System.out.println("Total Time: " + totalTime);
    }

    /**
     * Benchmarks memory mapping
     * @throws IOException
     */
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
        for(int i = 0; i < bufferRefills; i++) {
            innerloop:
            for(int j = 0; j < B; j++) {
                buffer[j] = rand.nextInt();
                counter++;
                if(counter >= N) {
                    break innerloop;
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
                channelObjects[i] = in[i].channelOpen(createFilename("testMapping",i),N);
                map[i] = channelObjects[i].getMap();
            } catch (FileNotFoundException e) {
                System.out.println("File not found!");
                return;
            }
        }


        for(int i = 0; i < k; i++) {
            int read = 0;
            while (!in[i].eof(map[i])) {
                int[] buf = in[i].read(map[i], Math.min(N-read, B));
                read += B;
                /* Uncomment this block if you want the result to be printed on screen */
//                for (int j = 0; j < buf.length; j++) {
//                    System.out.println(buf[j]);
//                }
//                System.out.println("**********");
            }
        }

        for(int i = 0; i < k; i++) {
            in[i].close(channelObjects[i]);
            try {
                deleteFiles("testMapping",i);
            } catch(java.nio.file.FileSystemException e) {

            }
        }

        endReadTime = System.currentTimeMillis();
        readTime = endReadTime - startReadTime;
        System.out.println("Read Time: " + readTime);
        totalTime = readTime + writeTime;
        System.out.println("Total Time: " + totalTime);
    }
  }
