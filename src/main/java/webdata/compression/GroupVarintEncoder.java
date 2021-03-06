package webdata.compression;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;

/** An encoder for encoding positive integers between 1-4 bytes(up to 2^32) using a variable amount of bytes
 *  Note that the value 0 cannot be encoded, as it is a sentinel value, acting as delimiter between posting lists or
 *  doubles as EOF in case the last group has less than 4 elements.
 *
 *  (This is OK in practice as we're encoding gaps/frequencies, which cannot be 0.)
 **/
public class GroupVarintEncoder extends OutputStream {

    private final OutputStream outputStream;

    private long numBytesWritten;

    // Holds up to 4 numbers
    private final int[] numbers;
    private int numbersInGroup;

    static final int BOUNDARY_8 =  1 << 8;
    static final int BOUNDARY_16 = 1 << 16;
    static final int BOUNDARY_24 = 1 << 24;

    /** Returns total number of bytes written(but not necessarily flushed)
     *  by this decoder into its associated output stream. */
    public long getTotalNumBytesWritten() {
        return numBytesWritten;
    }

    private void writeGroup() throws IOException
    {
        if (numbersInGroup == 0) {
            return;
        }
        int lengthByte = 0;
        int numBytes = 1;
        for (int i=0; i < 4; ++i)
        {
            int num = numbers[i];
            if (num < BOUNDARY_8)
            {
                lengthByte = lengthByte << 2;
                numBytes += 1;

            } else if (num < BOUNDARY_16)
            {
                lengthByte = (lengthByte << 2) | 1;
                numBytes += 2;
            } else if (num < BOUNDARY_24)
            {
                lengthByte = (lengthByte << 2) | 2;
                numBytes += 3;
            } else
            {
                lengthByte = (lengthByte << 2) | 3;
                numBytes += 4;
            }
        }


        byte[] bytesToWrite = new byte[numBytes];

        // ensure the length byte(encoded in an int) only uses the 8 lowest bits
        assert lengthByte == (lengthByte & 0xff);
        bytesToWrite[0] = (byte)lengthByte;
        int bytesSet = 1;

        for (int i=0; i < 4; ++i)
        {
            int num = numbers[i];
            // use big endian encoding, so the largest parts begin first
            if (num >= BOUNDARY_24)
            {
                bytesToWrite[bytesSet++] = (byte)((num >>> 24) & 0xff);
            }
            if (num >= BOUNDARY_16)
            {
                bytesToWrite[bytesSet++] = (byte)((num >>> 16) & 0xff);

            }
            if (num >= BOUNDARY_8)
            {
                bytesToWrite[bytesSet++] = (byte)((num >>> 8) & 0xff);
            }
            bytesToWrite[bytesSet++] = (byte)(num & 0xff);
        }

        assert bytesSet == numBytes;
        outputStream.write(bytesToWrite);
        numBytesWritten += numBytes;

        // no need to zero array, as next calls to write will just overwrite its contents
        numbersInGroup = 0;
    }

    @Override
    public void flush() throws IOException
    {
        finishPreviousGroup();
        outputStream.flush();

    }


//    private static final byte[] ALL_ZEROS = new byte[]{0, 0, 0, 0, 0};

    /** Writes the current group(if any) into the stream. This is similar to the {@link #flush()} method,
     *  except that the underlying stream is not flushed.
     * @throws IOException In case of IO error
     */
    public void finishPreviousGroup() throws IOException
    {
        assert numbersInGroup <= 4;
        if (numbersInGroup != 4)
        {
            // if the group has less than 4 elements
            // then numbers[numbersInGroup .. 4) might include junk/values from previous group,
            // zero them as 0 is a sentinel value indicating the stream has finished
            Arrays.fill(numbers, numbersInGroup, numbers.length, 0);
        }
        if (numbersInGroup != 0)
        {
            writeGroup();
        }
    }

    @Override
    public void close() throws IOException
    {
        flush();
        outputStream.close();
    }

    /** Creates a group varint encoder over a given stream
     * @param outputStream Stream where encoded integers will be written to. Will be flushed/closed
     *                     whenever the GroupVarintEncoder is flushed/closed, respectively.
     */
    public GroupVarintEncoder(OutputStream outputStream)
    {
        this.outputStream = outputStream;
        this.numBytesWritten = 0;
        this.numbers = new int[4];
        this.numbersInGroup = 0;
    }

    /** Allows writing an arbitrary integer. In contrast to the usual contract of OutputStream.write(int), numbers
     *  might utilize 31 bits. Negative and zero numbers cannot be encoded.
     * @param number A positive integer
     * @throws IOException In case the backing stream throws
     */
    @Override
    public void write(int number) throws IOException
    {
        if (number <= 0) {
            throw new IllegalArgumentException("Cannot encode zero or negative numbers");
        }
        numbers[numbersInGroup] = number;
        numbersInGroup++;
        if (numbersInGroup == 4)
        {
            writeGroup();
        }
    }
}
