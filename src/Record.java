/**
 * This class provides a data structure to insert integer into priority queue with it's file reference
 */

public class Record implements Comparable<Record> {
    private int value;
    private int fileReference;

    /**
     * Constructor
     * @param value
     * @param fileReference
     */

    public Record(int value, int fileReference)  {
        this.value = value;
        this.fileReference = fileReference;
    }

    public int getValue() {

        return value;
    }

    public int getFileReference() {
        return fileReference;
    }

    /**
     * Set the criteria for comparison
     * @param other integer to compare with
     * @return returns the difference between 2 values
     */

    public int compareTo(Record other){
        return this.value - other.getValue();
    }
}
