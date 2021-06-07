package webdata.sorting;

import webdata.Utils;
import webdata.storage.SerializableFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.function.Consumer;


/** Class for sorting large collections that don't fit in memory
 * @param <T> Element type
 * */
public class ExternalSorter<T> implements Closeable {

    private final SerializableFactory<T> serializableFactory;
    private final Runtime runtime;
    private final Comparator<T> comparator;
    private final Path workingDir;
    private static final long MIN_MEMORY_BYTES = 1024 * 1024 * 10;


    /**
     * @param factory Defines (de)serialization strategy for elements
     */
    public ExternalSorter(SerializableFactory<T> factory, Comparator<T> comparator, Path workingDir) throws IOException {
        this.serializableFactory = factory;
        this.runtime = Runtime.getRuntime();
        this.comparator = comparator;
        this.workingDir = workingDir;
        Files.createDirectories(workingDir);
    }

    private String getBlockFilepath(int numBlocks) {
        return workingDir.resolve("block" + numBlocks + ".bin").toString();
    }

    /**
     * Performs external sort on elements stored in a file
     *
     * @param dis Input stream containing serialized, non sorted elements. Not closed by this function.
     * @param dos Output stream for writing the sorted, serialized elements. Not closed by this function.
     */
    public void externalSort(DataInputStream dis, DataOutputStream dos) throws IOException {
        int blockNum = 0;
        Utils.log("Beginning external sort, working directory is %s", workingDir);
            boolean hasMoreElements = true;
            while (hasMoreElements)
            {
                ++blockNum;
                try (var blockFos = new FileOutputStream(getBlockFilepath(blockNum), false);
                     var blockDos = new DataOutputStream(new BufferedOutputStream(blockFos)))
                {
                    hasMoreElements = sortBlock(blockNum, dis, blockDos);
                }
            }
        Utils.log("Created %d sorted blocks at %s, beginning merge", blockNum, workingDir);
        mergeBlocks(blockNum, dos);
    }

    /**
     * Reads input from the stream until out of memory or stream is over(EOF),
     * returning whether there's more
     */
    private boolean sortBlock(final int blockNumber, DataInputStream dis,
                           DataOutputStream sortedBlockDos) throws IOException {
        var list = new ArrayList<T>();

        Utils.log("Starting to sort block %d, adding elements", blockNumber);
        boolean hasMoreElements = true;
        try {
            while (hasMemory(list)) {
                list.add(serializableFactory.deserialize(dis));
            }
        } catch (EOFException ex) {
            hasMoreElements = false;
        }
        Utils.log("Finished adding %,d elements to be sorted, hasMoreElements=%b",
                 list.size(), hasMoreElements);
        Utils.logMemory(runtime);
        list.sort(comparator);
        Utils.log("Sorted, writing to disk");
        for (var element: list) {
            serializableFactory.serialize(element, sortedBlockDos);
        }
        Utils.log("Serialized block %d to disk, block size: %,d bytes\n",
                  blockNumber, sortedBlockDos.size());
        return hasMoreElements;
    }

    /**
     * Performs multi-way merge, that is, merges multiple sorted sequences into a sorted sequence
     * @param splitsIt A finite collection of spliterators over sorted sequences
     * @param cmp Defines
     * @param <T> Element type
     * @return Spliterator of sorted elements
     */
    public static <T> Spliterator<T> merge(Iterable<Spliterator<T>> splitsIt, Comparator<T> cmp) {
        long sizeEstimate = 0;
        var spliterators = new ArrayList<Spliterator<T>>();
        int cs = 0;
        for (var split: splitsIt) {
            sizeEstimate += split.estimateSize();
            if (spliterators.size() == 0) {
                cs = split.characteristics();
            }
            cs &= split.characteristics() & (Spliterator.NONNULL | Spliterator.SIZED);
            spliterators.add(split);
        }
        cs |= Spliterator.ORDERED;

        final int numBlocks = spliterators.size();

        // A pair containing an element and the index of the spliterator it came from
        class ElementFrom {
            final T element;
            final int splitIx;
            ElementFrom(T element, int splitIx) {
                this.element = element;
                this.splitIx = splitIx;
            }

            T getElement() { return element; }
        }

        if (numBlocks == 0) {
            // For the edge case of no spliterators
            return Spliterators.emptySpliterator();
        }

        /* the heap includes one element from each spliterator, and the index of the spliterator */
        var heap = new PriorityQueue<ElementFrom>(numBlocks,
                Comparator.comparing(ElementFrom::getElement, cmp));

        // fill the heap
        for (int i=0; i < numBlocks; ++i) {
            int finalI = i;
            spliterators.get(i).tryAdvance(element -> {
                heap.add(new ElementFrom(element, finalI));
            });
        }

        return new Spliterators.AbstractSpliterator<T>(sizeEstimate, cs) {
            @Override
            public boolean tryAdvance(Consumer<? super T> action) {

                var elementFrom = heap.poll();
                if (elementFrom == null) {
                    return false;
                }
                int splitIx = elementFrom.splitIx;
                spliterators.get(elementFrom.splitIx).tryAdvance(newElem -> {
                    heap.add(new ElementFrom(newElem, splitIx));
                });

                action.accept(elementFrom.getElement());
                return true;
            }
        };
    }

    private void mergeBlocks(final int numBlocks, DataOutputStream dos) throws IOException {
        var fileStreams = new ArrayList<DataInputStream>();
        var elementStreams = new ArrayList<Spliterator<T>>();
        try {
            for (int blockNum = 1; blockNum <= numBlocks; ++blockNum) {
                var fis = new FileInputStream(getBlockFilepath(blockNum));
                var dis = new DataInputStream(new BufferedInputStream(fis));
                fileStreams.add(dis);
                elementStreams.add(serializableFactory.deserializeStream(dis));
            }


            var merged = merge(elementStreams, comparator);
            merged.forEachRemaining(min -> {
                try {
                    serializableFactory.serialize(min, dos);
                } catch (IOException e) {
                    throw new RuntimeException("IO error while serializing next merged sequence element", e);
                }
            });
        } finally {
            for (var file : fileStreams) {
                file.close();
            }
        }
    }

    /**
     * Check if we have enough memory to expand current block
     * @param curBlock Current block, may be used in override
     */
    protected boolean hasMemory(ArrayList<T> curBlock) {
        return Utils.getFreeMemory(runtime) >= MIN_MEMORY_BYTES;
    }

    @Override
    public void close() throws IOException {
        Utils.deleteDirectory(workingDir);
    }
}
