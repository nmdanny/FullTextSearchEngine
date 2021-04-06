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

    // Holds up to 4 numbers
    private final int[] numbers;
    private int numbersInGroup;

    static final int BOUNDARY_8 =  1 << 8;
    static final int BOUNDARY_16 = 1 << 16;
    static final int BOUNDARY_24 = 1 << 24;

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

        // no need to zero array, as next calls to write will just overwrite its contents
        numbersInGroup = 0;
    }

    @Override
    public void flush() throws IOException
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

        outputStream.flush();

    }


    private static final byte[] ALL_ZEROS = new byte[]{0, 0, 0, 0, 0};

    /** Resets the stream, which is similar to flushing, but ensures a 0 is written at the end.
     * @throws IOException
     */
    public void reset() throws IOException
    {
        if (numbersInGroup == 4)
        {
            flush();
            // If the last group(and thus all previous groups) is of size 4, then we haven't written any 0 value. We
            // need a delimiter value(doubles as EOF) so we explicitly write a 0.
            outputStream.write(ALL_ZEROS);
        } else
        {
            // Otherwise, a 0 was already written in the last group.
            flush();
        }
    }

    @Override
    public void close() throws IOException
    {
        flush();
        outputStream.close();
    }

    public GroupVarintEncoder(OutputStream outputStream)
    {
        this.outputStream = outputStream;
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
