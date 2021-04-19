package test;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import webdata.parsing.*;

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

        return new SequentialReviewParser(bufSize, DATASET_ENCODING).parse(path);
    }

    @Test
    void canParseDataset() throws IOException {
        var ds = fileToReviewStream("datasets\\1000.txt", 1024 * 10, 32)
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

    @Disabled
    @Test
    void canParse1GbDataset() throws IOException {
        int bufSize = 1024 * 1024 * 25;
        int numBufs = 4;
        var count = fileToReviewStream("E:\\webdata_datasets\\1gb.txt", bufSize, numBufs)
                .count();

        assertEquals(961098, count);

    }

    @Disabled
    @Test
    void canParse2GbDataset() throws IOException {
        int bufSize = 1024 * 1024 * 25;
        var path = "E:\\webdata_datasets\\2gb.txt";
        var stream1 = new SequentialReviewParser(bufSize, DATASET_ENCODING).parse(path);

        var it1 = stream1.iterator();

        long id = 0;
        while (it1.hasNext())
        {
            var _i1 = it1.next();
            ++id;
        }

        assertEquals(1924829, id);

    }

    @Disabled
    @Test
    void canParse4GbDataset() throws IOException {
        int bufSize = 1024 * 1024 * 25;
        int numBufs = 4;
        var count = fileToReviewStream("E:\\webdata_datasets\\4gb.txt", bufSize, numBufs)
                .count();

        assertEquals(3843236, count);

    }



    @Disabled
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