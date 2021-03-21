package webdata.parsing;

import java.util.regex.Pattern;

public class Tokenizer {
    static final Pattern NOT_ALPHANUM = Pattern.compile("\\W++");

    public static String[] tokenize(String raw)
    {
        String[] tokens = NOT_ALPHANUM.split(raw);
        for(int i=0; i < tokens.length; ++i)
        {
            tokens[i] = tokens[i].toLowerCase();
        }
        return tokens;
    }
}
