package webdata.parsing;

import webdata.Review;
import webdata.Utils;

import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CodingErrorAction;
import java.util.concurrent.Semaphore;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** A multi-threaded review parser for parsing large files that don't fit in memory
 */
public class ParallelReviewParser {

    final int bufSize;
    final int numBufs;
    final Charset charset;
    final Semaphore semaphore;
//    final CharBufferRental rental;

    public ParallelReviewParser(int bufSize, int numBufs, Charset charset)
    {
        this.bufSize = bufSize;
        this.numBufs = numBufs;
        this.charset = charset;
        this.semaphore = new Semaphore(numBufs);
//        this.rental = new CharBufferRental(numBufs, bufSize, charset);
    }

    public Stream<Review> parse(String filename) throws IOException
    {
        ByteBuffer delimiter = charset.encode("product/productId");
        var mmapBuffs = Utils.splitFile(filename, bufSize, delimiter)
                .collect(Collectors.toList());

        return mmapBuffs.stream().parallel()
                .flatMap(buf -> {
//                    CharBuffer charBuff = rental.rentBuff(buf);

//                    try {
//                        semaphore.acquire();
//                    } catch (InterruptedException e) {
//                        e.printStackTrace();
//                    }
                    var charBuff = charset.decode(buf);

//                    var reviews = InMemoryReviewParser.getReviewStream(charBuff)
//                            .Collectors.toList());

//                    semaphore.release();

//                    rental.returnBuff(charBuff);
                    return InMemoryReviewParser.getReviewStream(charBuff);
//                    return reviews.stream();
                });
    }
}
