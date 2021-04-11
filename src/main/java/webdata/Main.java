package webdata;

import webdata.dictionary.TermsManager;
import webdata.parsing.ParallelReviewParser;
import webdata.parsing.Review;
import webdata.parsing.SequentialReviewParser;
import webdata.parsing.Tokenizer;
import webdata.storage.CompactReview;
import webdata.storage.ReviewStorage;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.*;

public class Main {

    static final Charset DATASET_ENCODING = StandardCharsets.ISO_8859_1;
    public static void main(String[] args) throws IOException {

        var txtPath = Path.of("datasets", "1000.txt");
        var txtDirPath = Path.of("datasets", "1000");
        var writer = new SlowIndexWriter();
        writer.slowWrite(txtPath.toString(), txtDirPath.toString());

        var reader = new IndexReader(txtDirPath.toString());

        System.out.format("getTokenSizeOfReviews: %d\ngetNumberOfReviews: %d\n",
                reader.getTokenSizeOfReviews(),
                reader.getNumberOfReviews());


    }
}
