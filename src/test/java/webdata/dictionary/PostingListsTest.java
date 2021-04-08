package webdata.dictionary;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import webdata.inverted_index.PostingListReader;
import webdata.inverted_index.PostingListWriter;

import java.io.*;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

public class PostingListsTest {


    static Stream<Map<String, List<Integer[]>>> produceParameters() {
        var map1 = new HashMap<String, List<Integer[]>>();
        var abcList = new ArrayList<Integer[]>();
        abcList.add(new Integer[] { 1337, 50 });
        abcList.add(new Integer[] { 1338, 30 });
        abcList.add(new Integer[] { 2000, 3 });
        abcList.add(new Integer[] { 2001, 6000 });
        abcList.add(new Integer[] { 2001001, 19201080 });
        map1.put("abc", abcList);


        var tttList = new ArrayList<Integer[]>();
        tttList.add(new Integer[] { 5, 10});
        tttList.add(new Integer[] { 6, 11});
        tttList.add(new Integer[] { Integer.MAX_VALUE/2, Integer.MAX_VALUE});
        tttList.add(new Integer[] { Integer.MAX_VALUE, 5 });
        map1.put("ttt", tttList);

        var zzzList = new ArrayList<Integer[]>();
        zzzList.add(new Integer[] { 1, 10});
        map1.put("zzz", zzzList);

        return Stream.of(map1);
    }


    @ParameterizedTest
    @MethodSource("produceParameters")
    void canEncodeAndDecode(Map<String, List<Integer[]>> docIds) throws IOException {
        ensureCanEncodeAndDecode(docIds);
    }

    void ensureCanEncodeAndDecode(Map<String, List<Integer[]>> docIds) throws IOException {

        var file = Files.createTempFile("postingListTests", String.format("%d", docIds.hashCode()));

        var fileOs = new FileOutputStream(file.toString());
        var bufOs = new BufferedOutputStream(fileOs);

        var writer = new PostingListWriter(bufOs);

        Map<String, Integer> termToPostingPtr = new HashMap<>();

        for (var entry: docIds.entrySet()) {
            var pointer = writer.startTerm(entry.getKey());
            termToPostingPtr.put(entry.getKey(), pointer);
            for (var docIdAndFreq: entry.getValue()) {
                writer.add(docIdAndFreq[0], docIdAndFreq[1]);
            }
        }

        writer.close();

        var reader = new PostingListReader(file.toString());

        for (var entry: docIds.entrySet()) {
            var pointer = termToPostingPtr.get(entry.getKey());
            var frequency = entry.getValue().size() * 2;
            var numbersIt = reader.readDocIdFreqPairs(pointer, frequency);
            for (var docIdAndFreq: entry.getValue()) {
                int docId = numbersIt.nextElement();
                int freq = numbersIt.nextElement();
                assertEquals(docIdAndFreq[0], docId);
                assertEquals(docIdAndFreq[1], freq);
            }
        }

    }
}
