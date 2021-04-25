package webdata.spimi;

import webdata.Token;
import webdata.Utils;
import webdata.dictionary.Dictionary;
import webdata.dictionary.SequentialDictionaryBuilder;
import webdata.sorting.ExternalSorter;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Spliterator;
import java.util.stream.Stream;

/** Builds a dictionary via SPIMI algorithm, allowing
 *  processing token streams that are arbitrarily large.
 */
public class SPIMIIndexer {
    private final TemporaryIndexBuilder temporaryIndexBuilder;
    private final Path dir;
    private static final String TEMP_INDEX_DIR = "temp_indices";

    private static final long LOG_EVERY = 10000000L;

    public SPIMIIndexer(Path dir) throws IOException {
        this.dir = dir;
        this.temporaryIndexBuilder = new TemporaryIndexBuilder();

        Files.createDirectories(dir.resolve(TEMP_INDEX_DIR));

    }

    private Path pathForBlock(int blockNum) {
        return dir.resolve(TEMP_INDEX_DIR).resolve("tempIndex" + blockNum);
    }

    public void processTokens(Stream<Token> tokens) throws IOException {
        var it = tokens.iterator();

        int numIndices = 0;
        while (it.hasNext()) {
            ++numIndices;
            Utils.log("Creating temporary index number %d", numIndices);
            Utils.logMemory(Runtime.getRuntime());
            Path indexPath = pathForBlock(numIndices);
            Files.createDirectories(indexPath);
            temporaryIndexBuilder.invert(it, indexPath);
        }
        Utils.log("Merging final index from %d temporary indices", numIndices);
        Utils.log("Processed a total of %,d tokens", temporaryIndexBuilder.totalNumberOfTokens);

        merge(numIndices);
        Utils.log("Finished creating final index\n\n");
    }

    public void merge(int numIndices) throws IOException {
        var dicts = new ArrayList<Dictionary>();
        var tokenSplits = new ArrayList<Spliterator<Token>>();
        for (int i=1; i <= numIndices; ++i) {
            var dict = new Dictionary(pathForBlock(i).toString());
            dicts.add(dict);
            tokenSplits.add(dict.tokens());
        }

        var mergedStream = ExternalSorter.merge(tokenSplits,
                Comparator.comparing(Token::getTerm).thenComparing(Token::getDocID));

        long[] numTokensMerged = new long[]{0};
        try (var finalDictBuilder = new SequentialDictionaryBuilder(dir.toString())) {
            mergedStream.forEachRemaining(token -> {
                try {
                    finalDictBuilder.addToken(token);
                    numTokensMerged[0]++;
                    if (numTokensMerged[0] % LOG_EVERY == 0) {
                        Utils.log("So far merged a total of %,d tokens out of %,d",
                                numTokensMerged[0],
                                temporaryIndexBuilder.totalNumberOfTokens);
                    }
                } catch (IOException ex) {
                    throw new RuntimeException("IO error while building dictionary", ex);
                }
            });
        }
    }
}
