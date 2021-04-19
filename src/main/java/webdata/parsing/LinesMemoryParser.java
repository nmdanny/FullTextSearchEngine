package webdata.parsing;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

public class LinesMemoryParser {

    String productId;
    String helpfulness;
    String score;
    String text;

    final String productIdKey = "product/productId";
    final String helpfulnessKey = "review/helpfulness";
    final String scoreKey = "review/score";
    final String textKey = "review/text";

    public Stream<Review> parse(Path file, Charset cs) throws IOException {
        var lines = Files.lines(file, cs);
        return lines.sequential().filter(line -> {
            int colonPos = line.indexOf(":");
            if (colonPos == -1) {
                return false;
            }
            var key = line.substring(0, colonPos);
            var rest = line.substring(colonPos + 2);
            if (key.equals(productIdKey)) {
               productId = rest;
                return false;
            } else if (key.equals(helpfulnessKey)) {
                helpfulness = rest;
                return false;
            } else if (key.equals(scoreKey)) {
                score = rest;
                return false;
            } else if (key.equals(textKey)) {
                text = rest;
                return true;
            } else {
                return false;
            }
        }).map(_textLine -> {
            return Review.fromFields(productId, helpfulness, score, text);
        });
    }


}
