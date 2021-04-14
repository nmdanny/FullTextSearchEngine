package webdata.parsing;

import java.util.ArrayList;
import java.util.function.Predicate;
import java.util.regex.Pattern;

public class Tokenizer {
    static final Pattern NOT_ALPHANUM = Pattern.compile("\\W++", Pattern.UNICODE_CHARACTER_CLASS);

    public static String[] tokenize(CharSequence raw)
    {
        return NOT_ALPHANUM.splitAsStream(raw)
                .filter(Predicate.not(String::isEmpty))
                .map(String::toLowerCase)
                .toArray(String[]::new);
    }

}
