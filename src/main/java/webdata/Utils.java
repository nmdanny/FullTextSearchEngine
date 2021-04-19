package webdata;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class Utils {

    public static void log(String format, Object... args)
    {
        System.out.format(
                "[" + new SimpleDateFormat("HH:mm.ss").format(new Date()) + "] " +
                        format + "\n", args);
    }

    /** Returns the total amount of available memory to the program */
    public static long getFreeMemory(Runtime runtime) {
        // confusingly, 'freeMemory' is limited to the free memory out of
        // memory that was allocated to JVM(if -Xms isn't specified, it would be much
        // smaller than the total memory available)
        // See https://stackoverflow.com/a/18375641/5790380
        var usedMemory = runtime.totalMemory() - runtime.freeMemory();
        return runtime.maxMemory() - usedMemory;
    }

    public static void logMemory(Runtime runtime) {
        long maxMem = runtime.maxMemory();
        long totalMem = runtime.totalMemory();
        long freeMem = runtime.freeMemory();

        long usedMemory = totalMem - freeMem;
        long freeMemory = maxMem - usedMemory;

        double memUsage = 1.0 - ((double)freeMemory / maxMem);
        Utils.log("Memory usage: %.2f%% of %,d bytes", memUsage * 100, maxMem);
    }


    public static <T> Stream<T> iteratorToStream(Iterator<T> iterator)
    {
       return StreamSupport.stream(
               Spliterators.spliteratorUnknownSize(iterator, Spliterator.ORDERED),
               false
       );
    }

    public static <T> Enumeration<T> streamToEnumeration(Stream<T> stream)
    {
        var it = stream.iterator();
        return new Enumeration<T>() {
            @Override
            public boolean hasMoreElements() {
                return it.hasNext();
            }

            @Override
            public T nextElement() {
                return it.next();
            }
        };
    }

    public static void deleteDirectory(Path dirPath) throws IOException {
        if (!Files.exists(dirPath)) {
            return;
        }
        var deletedAllFiles = Files.walk(dirPath)
                .sorted(Comparator.reverseOrder())
                .map(Path::toFile)
                .reduce(true, (deleted, file) -> deleted && file.delete(), Boolean::logicalAnd);

        if (!deletedAllFiles) {
            System.err.println("Couldn't delete all files within directory\n");
        }
        Files.deleteIfExists(dirPath);
    }
}
