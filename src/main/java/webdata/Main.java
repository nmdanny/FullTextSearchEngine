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
        var bin = Path.of("E:", "webdata_datasets", "1gb.txt");
        try (var par_storage = new ReviewStorage(Path.of("E:", "webdata_datasets", "all_par-storage.bin"));
             var par_strings = new TermsManager(Path.of("E:", "webdata_datasets", "all_par-terms.bin").toString(), DATASET_ENCODING);
             var seq_storage = new ReviewStorage(Path.of("E:", "webdata_datasets", "all_seq-storage.bin"));
             var seq_strings = new TermsManager(Path.of("E:", "webdata_datasets", "all_seq-terms.bin").toString(), DATASET_ENCODING);
             ) {
            int bufSize = 1024 * 1024 * 25;
            int numBufs = 4;

            var par_strings_set = new TreeSet<String>();
            var par_parser = new ParallelReviewParser(bufSize, numBufs, DATASET_ENCODING)
                    .parse(bin.toString())
                    .sorted(Comparator.comparing(Review::getProductId))
                    .peek(review -> {
                        par_strings_set.addAll(Arrays.asList(Tokenizer.tokenize(review.getText())));
                    });


            var seq_strings_set = new TreeSet<String>();
            var seq_parser = new SequentialReviewParser(bufSize, DATASET_ENCODING)
                    .parse(bin.toString())
                    .sorted(Comparator.comparing(Review::getProductId))
                    .peek(review -> {
                        seq_strings_set.addAll(Arrays.asList(Tokenizer.tokenize(review.getText())));
                    });


            try {
                System.out.println("Begin parsing va parallel");
                par_storage.appendMany(par_parser.map(CompactReview::new));
            } catch (IOException e) {
                e.printStackTrace();
            }

            try {
                System.out.println("Begin t2");
                seq_storage.appendMany(seq_parser.map(CompactReview::new));
            } catch (IOException e) {
                e.printStackTrace();
            }

            System.out.println("Done t2");

            for (var string: par_strings_set) {
                par_strings.allocateTerm(string);
            }
            for (var string: seq_strings_set) {
                seq_strings.allocateTerm(string);
            }


            System.out.println("Done");
        }


    }
}
