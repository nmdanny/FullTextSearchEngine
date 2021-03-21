package webdata.compression;

import java.io.IOException;
import java.io.OutputStream;

/** An encoder for encoding integers between 1-4 bytes(up to 2^32) using a variable amount of bytes */
public class GroupVarintEncoder extends OutputStream {

    private OutputStream outputStream;

    // Holds up to 4 numbers
    private int[] numbers;
    private int numbersInGroup;

    static final int BOUNDARY_8 =  1 << 8;
    static final int BOUNDARY_16 = 1 << 16;
    static final int BOUNDARY_24 = 1 << 24;

    private void writeFullGroup() throws IOException
    {
        assert numbersInGroup == 4;
        int lengthByte = 0;
        int numBytes = 1;
        for (int i=0; i < numbersInGroup; ++i)
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

        assert lengthByte == (byte)lengthByte;
        bytesToWrite[0] = (byte)lengthByte;
        int bytesSet = 1;

        for (int i=0; i < numbersInGroup; ++i)
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
        this.numbersInGroup = 0;
    }

    @Override
    public void flush() throws IOException
    {
        assert numbersInGroup <= 4;

        if (numbersInGroup == 4)
        {
            writeFullGroup();
        }

        else {
            System.err.println("TODO write non full group");
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
        numbers[numbersInGroup] = number;
        numbersInGroup++;
        if (numbersInGroup == 4)
        {
            writeFullGroup();
        }
    }
}
