package webdata;

import java.io.IOException;

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
