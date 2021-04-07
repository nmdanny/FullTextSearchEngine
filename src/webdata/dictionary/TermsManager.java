package webdata.dictionary;

import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.file.Path;

/** A tuple containing the position and length(in bytes) of some term which was saved in the file */
class TermAllocationResult {
    final int position;
    final int length;

    TermAllocationResult(int position, int length) {
        this.position = position;
        this.length = length;
    }
}

/** Manages random access to terms (reading and writing) via memory mapped file */
public class TermsManager implements Closeable, Flushable {
    private static final String TERMS_FILE_NAME = "terms.bin";
    private static final int MMAP_SIZE = Integer.MAX_VALUE;
    private final Charset charset;

    private final RandomAccessFile randomAccessFile;
    private final FileChannel fileChannel;
    private final MappedByteBuffer page;

    public TermsManager(String dir, Charset charset) throws IOException {
        this.charset = charset;
        this.randomAccessFile = new RandomAccessFile(Path.of(dir, TERMS_FILE_NAME).toString(), "rw");
        this.fileChannel = randomAccessFile.getChannel();
        this.page = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, MMAP_SIZE);
    }

    /** Allocates a new term into the terms file and memory, returning a term pointer */
    public TermAllocationResult allocateTerm(String term) throws IOException {
        ByteBuffer bytes = charset.encode(term);
        int pos = page.position();
        int length = bytes.limit();
        var result = new TermAllocationResult(pos, length);
        page.put(bytes);
        return result;
    }

    /** Dereferences the term at given position and length */
    public CharBuffer derefTerm(int position, int length) {
        ByteBuffer bytes = page.duplicate()
                .position(position)
                .limit(position + length);
        return charset.decode(bytes);
    }

    public CharBuffer derefTerm(TermAllocationResult alloc) {
        return derefTerm(alloc.position, alloc.length);
    }


    @Override
    public void close() throws IOException {
        fileChannel.close();
        randomAccessFile.close();
    }

    @Override
    public void flush() throws IOException {
        fileChannel.force(true);
    }
}
