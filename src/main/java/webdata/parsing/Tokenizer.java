package webdata.parsing;

import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Stream;

public class Tokenizer {
    static final Pattern NOT_ALPHANUM = Pattern.compile("\\W++", Pattern.UNICODE_CHARACTER_CLASS);

    public static String[] tokenize(CharSequence raw)
    {
        return tokensAsStream(raw)
                .toArray(String[]::new);
    }

    public static Stream<String> tokensAsStream(CharSequence raw)
    {
        return NOT_ALPHANUM.splitAsStream(raw)
                .filter(Predicate.not(String::isEmpty))
                .map(String::toLowerCase);
    }

}
