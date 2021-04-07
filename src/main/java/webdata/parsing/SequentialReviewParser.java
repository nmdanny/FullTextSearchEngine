package webdata.parsing;

import webdata.Review;
import webdata.Utils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Scanner;
import java.util.stream.Stream;

/** A single threaded review parser that can handle arbitrarily large files */
public class SequentialReviewParser {
    final int bufSize;
    final Charset charset;

    public SequentialReviewParser(int bufSize, Charset charset)
    {
        this.bufSize = bufSize;
        this.charset = charset;
    }

    public Stream<Review> parse(String file) throws IOException {
        var reader = new BufferedReader(new FileReader(file, charset), bufSize);
        var scanner = new Scanner(reader);
        scanner.useDelimiter("(?=product/productId)");
        var parser = new InMemoryReviewParser("");

        return scanner.tokens()
                .flatMap(stream -> {
                    parser.reset(stream);
                    return Utils.iteratorToStream(parser);
                })
                .onClose(scanner::close);
    }
}
