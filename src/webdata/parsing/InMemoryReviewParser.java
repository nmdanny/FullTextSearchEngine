package webdata.parsing;

import webdata.Review;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/** A review parser that operates in a single thread upon a CharSequence(which is probably
 *  allocated within memory)
 *
 *  Note: The provided CharSequence mustn't be changed until the iterator is completely consumed or reset
 */
public class InMemoryReviewParser implements Iterator<Review> {
    // Used to parse a field of a product review
    private final Pattern FIELD_PATTERN = Pattern.compile(
            "(?:product|review)/(?<fieldKey>\\w++):(?<fieldValue>.*+)"
    );

    // Assume each review begins with a `productId`, followed by the rest of the fields we're interested in.
    private final String DELIMITER_FIELD_KEY = "productId";

    private final Matcher matcher;
    private boolean hasMatch;

    public InMemoryReviewParser(CharSequence seq) {
        this.matcher = FIELD_PATTERN.matcher(seq);
        this.hasMatch = this.matcher.find();
    }

    /// For reusing the parser with a new sequence
    public void reset(CharSequence newSeq)
    {
        this.matcher.reset(newSeq);
        this.hasMatch = this.matcher.find();
    }

    public static Stream<Review> getReviewStream(CharSequence seq)
    {
        var parser = new InMemoryReviewParser(seq);
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(parser, Spliterator.ORDERED),
                false);
    }

    @Override
    public boolean hasNext() {
        return hasMatch && matcher.group("fieldKey").equals(DELIMITER_FIELD_KEY);
    }

    @Override
    public Review next() {
        if (!hasNext())
        {
            throw new NoSuchElementException();
        }

        var fields = new HashMap<String, String>();
        do {
            fields.put(matcher.group("fieldKey"), matcher.group("fieldValue"));
            hasMatch = matcher.find();
        }
        while (hasMatch && !matcher.group("fieldKey").equals(DELIMITER_FIELD_KEY));

        return Review.fromFields(fields);
    }


}
