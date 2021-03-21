package test;

import org.junit.jupiter.api.Test;
import webdata.Review;
import webdata.parsing.ParallelReviewParser;
import webdata.parsing.SequentialReviewParser;
import webdata.parsing.InMemoryReviewParser;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

class ReviewParserTest {

    static final Charset DATASET_ENCODING = StandardCharsets.ISO_8859_1;

    static Stream<Review> fileToReviewStream(String path, int bufSize, int numBufs) throws IOException {

        return new ParallelReviewParser(bufSize, numBufs, DATASET_ENCODING).parse(path);
    }

    @Test
    void canParseDataset() throws IOException {
        var ds = fileToReviewStream("datasets\\1000.txt", 1024 * 10, 32)
                .peek(System.out::println)
                .collect(Collectors.toList());

            var raw = Files.readString(Path.of("datasets", "1000.txt"), DATASET_ENCODING);
        var ds2 = InMemoryReviewParser.getReviewStream(raw)
                .collect(Collectors.toList());

        assertEquals(1000, ds.size());
        assertIterableEquals(ds, ds2);
    }

    @Test
    void canParse1MbDataset() throws IOException {
        int bufSize = 1024 * 256;
        int numBufs = 4;
        var count = fileToReviewStream("E:\\webdata_datasets\\1mb.txt", bufSize, numBufs)
                .count();

        assertEquals(1079, count);
    }

    @Test
    void canParse100MbDataset() throws IOException {
        int bufSize = 1024 * 1024;
        int numBufs = 4;
        var count = fileToReviewStream("E:\\webdata_datasets\\100mb.txt", bufSize, numBufs)
                .count();

        assertEquals(92427, count);
    }

    @Test
    void canParse1GbDataset() throws IOException {
        int bufSize = 1024 * 1024 * 25;
        int numBufs = 4;
        var count = fileToReviewStream("E:\\webdata_datasets\\1gb.txt", bufSize, numBufs)
                .count();

        assertEquals(961098, count);

    }

    @Test
    void canParse2GbDataset() throws IOException {
        int bufSize = 1024 * 1024 * 25;
        int numBufs = 4;
        var path = "E:\\webdata_datasets\\2gb.txt";
        var stream1 = new ParallelReviewParser(bufSize, numBufs, DATASET_ENCODING).parse(path);
        var stream2 = new SequentialReviewParser(bufSize, DATASET_ENCODING).parse(path);

        var it1 = stream1.iterator();
        var it2 = stream2.iterator();

        long id = 0;
        while (it1.hasNext() || it2.hasNext())
        {
            if (it1.hasNext() && it2.hasNext())
            {
                var i1 = it1.next();
                var i2 = it2.next();
                assertEquals(i1, i2, "at index " + id);
            } else if (it1.hasNext())
            {
                System.err.println("stream1(parallel) is longer: " + it1.next());
            } else {
                System.err.println("stream2(sequential) is longer: " + it2.next());
            }
            ++id;
        }

//        var ds2 =
//        var raw = Files.readString(Path.of("E:", "webdata_datasets", "2gb.txt"), DATASET_ENCODING);
//        var ds2 = InMemoryReviewParser.getReviewStream(raw)
//                .collect(Collectors.toList());

        assertEquals(1924829, id);
//        assertEquals(1924829, count);

    }

    @Test
    void canParse4GbDataset() throws IOException {
        int bufSize = 1024 * 1024 * 25;
        int numBufs = 4;
        var count = fileToReviewStream("E:\\webdata_datasets\\4gb.txt", bufSize, numBufs)
                .count();

        assertEquals(3843236, count);

    }



    @Test
    void canParse8GbDataset() throws IOException {
        int bufSize = 1024 * 1024 * 25;
        int numBufs = 4;
        var id = new AtomicLong(0);
        var count = fileToReviewStream("E:\\webdata_datasets\\8gb.txt", bufSize, numBufs)
//                .peek(review -> {
//                    var index = id.getAndAdd(1);
//                    if (index < 10 || index >= 7850065) {
//                        System.out.println("At index " + index + ": " + review);
//                    }
//                })
                .count();

        assertEquals(7850072, count);
    }
}