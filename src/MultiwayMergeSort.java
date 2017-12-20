import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

public class MultiwayMergeSort {

    private int M; // Number of integers that fit in memory
    private int d; // Streams to open at once
    private int N; // Initial file size in integers
    private static int fileNumber; // The file number for Phase 1 files; after phase 1 it includes the number of files
    private Queue<Integer> fileNumbers = new ArrayDeque<>(); // Queue for the filenames' numbers
    HashMap<Integer,Integer> integersPerFile = new HashMap<>();  // Overall number of integers in file
    
    public MultiwayMergeSort(int M, int d, int N) {
        fileNumber = 0;
        this.M = M;
        this.d = d;
        this.N = N;
    }


    public int mergeSort(String initialFilePath) throws IOException {
        /* First Phase: Splitting the file into N/M sorted files */
        splitFiles(initialFilePath);

        /* Second Phase */
        int B = (int)Math.ceil(M / (d+1)); // The memory should fit d blocks for files and 1 block for output; each block should be of size B
        int iteration = 1;
        /* Memory of M integers consists of the priority queue and the output buffer*/
        /*In total the size of both structures is M integers*/
        PriorityQueue<Record> priorityQueue = new PriorityQueue<>();
        int[] outputBuffer = new int[B]; // Buffer in memory to store the output
        int outputBufferCount = 0; // Counter on output buffer to check if buffer is full

        // While there are more files to be merge-sorted; each loop produces one output file
        while(fileNumbers.size() > 1) {
            System.out.println("Iteration:" + iteration);

            HashMap<Integer, InStream> in = new HashMap<>();
            HashMap<Integer, ChannelObjects> channelObjects = new HashMap<>(); //reference number for all the open files
            HashMap<Integer, Integer> fileFrequencies = new HashMap<>(); //number of integers of a particular file in the queue

            /* File opening for read operation */
            for(int i = 0; i < d; i++) {
                // TODO: 20/12/2017 Keep in mind that last file can be smaller and thus correction might be needed
                int fileNumber = fileNumbers.peek();
                fileFrequencies.put(fileNumber, 0);
                in.put(fileNumber,new InStream());
                channelObjects.put(fileNumber, in.get(fileNumber).channelOpen(IOBenchmarking.createFilename("file",fileNumber),integersPerFile.get(fileNumber)));
                fileNumbers.poll();

            }

             /* Create file to write output */
            OutStream out = new OutStream();
            ChannelObjects channelObject;
            channelObject = out.channelCreate(IOBenchmarking.createFilename("file",fileNumber), (int)Math.pow(M,iteration+1));
            fileNumbers.add(fileNumber);
            integersPerFile.put(fileNumber,0);

            // Add  first block of every file to the priority queue
            for(int file:channelObjects.keySet()){
                // TODO: 20/12/2017 What if the end block has less than B integers?
                int[] buffer = new int[B];
                try {
                   buffer = in.get(file).read(channelObjects.get(file).getMap(), B);
                } catch(java.nio.BufferUnderflowException e) {

                }
                for(int j = 0; j < buffer.length; j++) {
                    priorityQueue.add(new Record(buffer[j], file));
                    //increment number of integers from one file in priority queue
                    fileFrequencies.put(file, fileFrequencies.get(file)+1);
                    integersPerFile.put(file, integersPerFile.get(file)+1);
                }
            }

            //while priority has numbers, put them in the output buffer
            while(!priorityQueue.isEmpty()) {
                Record record = priorityQueue.poll();
                int fileNumber = record.getFileReference();
                //decrement number of integers from one file in priority queue
                fileFrequencies.put(fileNumber, fileFrequencies.get(fileNumber)-1);
                outputBuffer[outputBufferCount] = record.getValue();
                outputBufferCount++;
                //if output buffer is full, write it to file
                if(B == outputBufferCount) {
                    out.write(channelObject.getMap(), outputBuffer);
                    outputBufferCount = 0;
                    integersPerFile.put(fileNumber,integersPerFile.get(fileNumber) + B);
                }
                //if the file block is finished from priority queue, bring the next block from file (if available)
                if(fileFrequencies.get(fileNumber)== 0) {
                    if(!in.get(fileNumber).eof(channelObjects.get(fileNumber).getMap())) {
                        System.out.println("File number: " + fileNumber);
                        int[] buffer = new int[B];
                        buffer = in.get(fileNumber).read(channelObjects.get(fileNumber).getMap(), B);
                        for(int j = 0; j < buffer.length; j++) {
                            priorityQueue.add(new Record(buffer[j], fileNumber));
                            //increment number of integers from one file in priority queue
                            fileFrequencies.put(fileNumber, fileFrequencies.get(fileNumber)+1);
                        }
                    } else {
                        fileFrequencies.remove(fileNumber);
                        in.get(fileNumber).close(channelObjects.get(fileNumber));
                        channelObjects.remove(fileNumber);
                        in.remove(fileNumber);
//                      Files.deleteIfExists(Paths.get(IOBenchmarking.createFilename("file",fileNumber)));
                    }
                }
            }

            // TODO: 20/12/2017 Check the output buffer for remaining records

            if(outputBufferCount != 0) {
                integersPerFile.put(fileNumber,integersPerFile.get(fileNumber) + outputBufferCount);
                out.write(channelObject.getMap(), outputBuffer);
            }
            out.close(channelObject);
            iteration+=1;
            System.out.println("Integers in file " + fileNumber + ": " + integersPerFile.get(fileNumber));
            fileNumber++;
        }
        return fileNumber-1;
    }

    public void splitFiles(String initialFilePath) throws IOException {
        InStream input = new InStream();
        ChannelObjects inputChannelObject = input.channelOpen(initialFilePath,N);

        ChannelObjects outputChannelObject;
        
        while(!input.eof(inputChannelObject.getMap())) { // While there are more blocks to read
            /* Read Block */
            int[] block = input.read(inputChannelObject.getMap(),M); // Read one block at a time
            Arrays.sort(block); // Sort the block
            
            /* Write Block */
            String filename = IOBenchmarking.createFilename("file", fileNumber); // The file to be written
            fileNumbers.add(fileNumber);
            integersPerFile.put(fileNumber,block.length);
            System.out.println("Rows for file " + fileNumber + ": " + block.length);
            fileNumber+=1;
            OutStream output = new OutStream();
            outputChannelObject = output.channelCreate(filename, M);
            output.write(outputChannelObject.getMap(),block); // Write block
            // TODO: 20/12/2017 Last file rows are not equal to M
            output.close(outputChannelObject);
        }
    }

}
