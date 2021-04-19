package webdata.storage;

import webdata.sorting.ExternalSorter;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.AbstractList;
import java.util.Comparator;

/** Allows treating a file containing fixed size records as a list,
 *  allowing random access via index, binary search(if sorted), as well as appending elements(as long as they maintain order).
 * @param <Record> Type of record
 */
public class FixedRecordStorage<Record> extends AbstractList<Record> implements Flushable, Closeable {
    private final SerializableFactory<Record> recordFactory;

    // used to enforce append order(aka, exists purely for debugging/sanity checking), if not null
    private final Comparator<Record> comparator;

    // Path of records file
    private final String path;

    // allows reading from storage
    private RandomAccessFile file;
    private FileChannel channel;
    private final ByteBuffer readBuf;

    // allows appending records to storage
    private DataOutputStream insertStream;

    private int numRecords;

    private final int sizePerRecord;
    private Record lastRecord;


    /**
     * Creates a fixed record storage
     * @param filepath Path to records file
     * @param factory Defines (de)serialization
     * @param comparator If not null, used in asserts when adding elements
     * @throws IOException In case of IO error when accessing records file
     */
    FixedRecordStorage(String filepath, SerializableFactory<Record> factory, Comparator<Record> comparator) throws IOException {
        this.recordFactory = factory;
        this.comparator = comparator;
        this.path = filepath;
        this.sizePerRecord = factory.sizeBytes();
        assert sizePerRecord > 0;

        this.readBuf = ByteBuffer.allocate(sizePerRecord);
        loadFile(filepath);
    }

    private void loadFile(String filepath) throws IOException {
        this.file = new RandomAccessFile(filepath, "rw");
        this.channel = file.getChannel();
        assert (file.length() % sizePerRecord == 0) : "Record file in improper format";
        assert (file.length() / sizePerRecord <= Integer.MAX_VALUE) : "Record file contains too many records, can only have 2^31 records";

        this.numRecords = (int)(file.length() / sizePerRecord);
        this.insertStream = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(filepath, true)));

        this.lastRecord = null;
        if (this.numRecords > 0) {
            this.lastRecord = get(size() - 1);
        }
    }

    @Override
    public boolean add(Record record) {
        try {
            assert (lastRecord == null || comparator == null ||
                    (comparator.compare(record, lastRecord)) >= 0) : "Records must be inserted in ascending order";
            recordFactory.serialize(record, insertStream);
            lastRecord = record;
            numRecords += 1;
            return true;
        } catch (IOException ex) {
            throw new RuntimeException("Error while adding record " + record, ex);
        }
    }

    @Override
    public Record get(int index) {
        long filePos = (long)index * sizePerRecord;
        try {
            channel.position(filePos);
            readBuf.clear();
            int bytesRead = channel.read(readBuf);
            readBuf.flip();
            assert (bytesRead == sizePerRecord) : "Couldn't read all bytes";
            return recordFactory.deserialize(readBuf);
        } catch (IOException e) {
            throw new RuntimeException("Couldn't get record at index " + index, e);
        }
    }

    @Override
    public int size() {
        return numRecords;
    }

    /**
     * Performs external sort, replacing the file containing the records with a new one.
     * Invalidates any methods which are reliant on old file handles.
     * @param comparator Defines ordering
     * @throws IOException Thrown in case of IO error
     */
     public void externalSort(Comparator<Record> comparator) throws IOException {
         close();
         var folder = Path.of(path).getParent();
         var filename = Path.of(path).getFileName();
         var workDirPath = folder.resolve(filename + "-externalSort");
         try (var nonSortedIs = new BufferedInputStream(new FileInputStream(path));
              var nonSortedDis = new DataInputStream(nonSortedIs);
              var sortedOs = new BufferedOutputStream(new FileOutputStream(path + "sorted", false));
              var sortedDos = new DataOutputStream(sortedOs);
              var sorter = new ExternalSorter<Record>(recordFactory, comparator, workDirPath)
         )
         {
             sorter.externalSort(nonSortedDis, sortedDos);
         }
         Files.move(Path.of(path + "sorted"), Path.of(path), StandardCopyOption.REPLACE_EXISTING);
         loadFile(path);
    }

    @Override
    public void close() throws IOException {
        flush();
        this.insertStream.close();
        this.channel.close();
        this.file.close();
    }

    @Override
    public void flush() throws IOException {
        this.insertStream.flush();
        this.channel.force(true);
    }
}
