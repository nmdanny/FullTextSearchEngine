package webdata.compression;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;

/** An encoder for encoding positive integers between 1-4 bytes(up to 2^32) using a variable amount of bytes
 *  Note that the value 0 cannot be encoded, but this is OK because we're encoding gaps, which cannot be 0.
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

        assert lengthByte == (lengthByte & 0xff);
        bytesToWrite[0] = (byte)lengthByte;
        int bytesSet = 1;

        for (int i=0; i < 4; ++i)
        {
            int num = numbers[i];
            // use big endian encoding, so the largest parts begin first
            if (num >= BOUNDARY_24)
            {
                bytesToWrite[bytesSet++] = (byte)(num >> 24);
            }
            if (num >= BOUNDARY_16)
            {
                bytesToWrite[bytesSet++] = (byte)(num >> 16);

            }
            if (num >= BOUNDARY_8)
            {
                bytesToWrite[bytesSet++] = (byte)(num >> 8);
            }
            bytesToWrite[bytesSet++] = (byte)(num);
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
