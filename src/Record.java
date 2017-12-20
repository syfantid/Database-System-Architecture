public class Record implements Comparable<Record> {
    private int value;
    private int fileReference;

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

    public int compareTo(Record other){
        return this.value - other.getValue();
    }
}
