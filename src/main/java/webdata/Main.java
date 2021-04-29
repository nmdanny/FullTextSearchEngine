package webdata;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

class Analysis {

    final List<Entry> entries = new ArrayList<>();

    static class Entry {
        String dir;
        int numReviews;
        int numTokens;
        String operation;
        int iterations;
        long totalTimeNs;
    }

    final int NUM_CALLS = 100;

    void measureDir(String dir) throws IOException {
        var reader = new IndexReader(dir);
        var tokens = StreamSupport.stream(reader.dictionary.terms(), false)
                .map(Map.Entry::getKey)
                .collect(Collectors.toList());
        Collections.shuffle(tokens);

        assert tokens.size() >= NUM_CALLS && reader.getTokenSizeOfReviews() >= tokens.size();

        BiConsumer<String, Consumer<Integer>> measure = (opName, op) -> {
            var start = System.nanoTime();
            Utils.log("Beginning test for %s, index: %s", opName, dir);
            for (int i=0; i < NUM_CALLS; ++i) {
                op.accept(i);
            }
            var deltaNs = System.nanoTime() - start;
            Entry entry = new Entry();
            entry.dir = dir;
            entry.numReviews = reader.getNumberOfReviews();
            entry.numTokens = reader.getTokenSizeOfReviews();
            entry.operation = opName;
            entry.iterations = NUM_CALLS;
            entry.totalTimeNs = deltaNs;
            entries.add(entry);
            Utils.log("Took %d nanoseconds", deltaNs);
        };


        measure.accept("getReviewsWithToken", i -> {
            var enumm = reader.getReviewsWithToken(tokens.get(i));
            int numEntries = 0;
            while (enumm.hasMoreElements()) {
                enumm.nextElement();
                ++numEntries;
            }
            assert numEntries > 0 && numEntries % 2 == 0;
        });

        measure.accept("getTokenFrequency", i -> {
            int freq = reader.getTokenFrequency(tokens.get(i));
            assert freq > 0;
        });
    }

    static String CSV_PATH = "analysis\\analysis.csv";

    void dump_csv() throws IOException {

        boolean fileExists = Files.exists(Path.of(CSV_PATH));
        var file = new FileWriter("analysis\\analysis.csv", true);

        if (!fileExists) {
            var fields = List.of("dir", "numReviews", "numTokens", "operation", "iterations", "totalTimeNs");
            file.write(String.join(",", fields) + "\n");
        }
        for (var entry: entries) {
            file.write(String.format("%s,%d,%d,%s,%d,%d\n",
                    entry.dir, entry.numReviews, entry.numTokens, entry.operation, entry.iterations, entry.totalTimeNs));
        }
        file.close();
    }
}

public class Main {

    public static void main(String[] args) throws IOException {

        var analyzer = new Analysis();
        var dirs = List.of(
                Path.of("E:", "webdata_datasets", "all-long-varint"),
                Path.of("E:", "webdata_datasets", "1000"),
                Path.of("E:", "webdata_datasets", "music-long"),
                Path.of("E:", "webdata_datasets", "videogames")
        );
        for (var dir: dirs) {
            analyzer.measureDir(dir.toString());
        }
        analyzer.dump_csv();

//        var inputFile = args[0];
//        var indexDir = args[1];
//
//        var writer = new SlowIndexWriter();
//        writer.slowWrite(inputFile, indexDir);
//
//        var reader = new IndexReader(indexDir);
//
//
//        var entries = new HashMap<>();
//
//        System.out.format("getTokenSizeOfReviews: %d\ngetNumberOfReviews: %d\n",
//                reader.getTokenSizeOfReviews(),
//                reader.getNumberOfReviews());
//
//        var max = new Object[]{0, ""};
//        reader.dictionary.terms().forEachRemaining(entry -> {
//            var term = entry.getKey();
//            if (term.length() > (int) max[0]) {
//                max[0] = term.length();
//                max[1] = term;
//                Utils.log("Found longer token \"%s\" of length %d", term, term.length());
//                if (term.length() <= 100) {
//                    return;
//                }
//                var it = reader.getReviewsWithToken(term);
//                while (it.hasMoreElements()) {
//                    int docId = it.nextElement();
//                    int freq = it.nextElement();
//                    Utils.log("\t\tappears at docId %d with freq %d", docId, freq);
//                }
//            }
//        });


    }
}
