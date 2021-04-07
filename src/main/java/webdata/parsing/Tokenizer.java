package webdata.parsing;

import java.util.regex.Pattern;

public class Tokenizer {
    static final Pattern NOT_ALPHANUM = Pattern.compile("\\W++");
    static final Pattern NOT_UNICODE_WORD = Pattern.compile("\\W++", Pattern.UNICODE_CHARACTER_CLASS);

    static final boolean TOKENIZE_UNICODE_BY_DEFAULT = true;

    public static String[] tokenize(CharSequence raw) {
        return tokenize(raw, TOKENIZE_UNICODE_BY_DEFAULT);
    }

    public static String[] tokenize(CharSequence raw, boolean unicode)
    {
        String[] tokens = (unicode ? NOT_UNICODE_WORD : NOT_ALPHANUM).split(raw);
        for(int i=0; i < tokens.length; ++i)
        {
            tokens[i] = tokens[i].toLowerCase();
        }
        return tokens;
    }

}
