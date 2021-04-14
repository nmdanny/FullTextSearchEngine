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

    public static void main(String[] args) throws IOException {

        var inputFile = args[0];
        var indexDir = args[1];

        var writer = new SlowIndexWriter();
        writer.slowWrite(inputFile, indexDir);

        var reader = new IndexReader(indexDir);

        System.out.format("getTokenSizeOfReviews: %d\ngetNumberOfReviews: %d\n",
                reader.getTokenSizeOfReviews(),
                reader.getNumberOfReviews());


    }
}
