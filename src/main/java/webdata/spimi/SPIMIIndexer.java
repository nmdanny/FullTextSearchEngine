package webdata.spimi;

import webdata.Token;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/** Builds a dictionary via SPIMI algorithm, allowing
 *  processing token streams that are arbitrarily large.
 */
public class SPIMIIndexer {
    private final TemporaryIndexBuilder temporaryIndexBuilder;
    private final Path dir;
    private static final String TEMP_INDEX_DIR = "temp_indices";

    public SPIMIIndexer(Path dir) throws IOException {
        this.dir = dir;
        this.temporaryIndexBuilder = new TemporaryIndexBuilder();

        Files.createDirectories(dir.resolve(TEMP_INDEX_DIR));

    }

    private String pathForBlock(int blockNum) {
        return dir.resolve(TEMP_INDEX_DIR).resolve("tempIndex" + blockNum + ".bin").toString();
    }

    public void processTokens(Stream<Token> tokens) throws IOException {
        var it = tokens.iterator();

        int numBlocks = 0;
        while (it.hasNext()) {
            ++numBlocks;
            String tempIndexPath = pathForBlock(numBlocks);
            try (var os = new BufferedOutputStream(new FileOutputStream(tempIndexPath, false))) {
                temporaryIndexBuilder.invert(it, os);
            }
        }
        var filePaths = IntStream.rangeClosed(1, numBlocks)
                .mapToObj(this::pathForBlock).collect(Collectors.toList());
        Merger.merge(dir.toString(), filePaths);
    }
}
