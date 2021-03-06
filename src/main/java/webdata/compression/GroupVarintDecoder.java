package webdata.compression;

import java.io.IOException;
import java.io.InputStream;

public class GroupVarintDecoder extends InputStream {
    private final InputStream inputStream;

    // contains the decoded values of the current group(or 0 if no group was read yet)
    private final int[] curGroup;

    // a buffer used for decoding a group. An optimization so we won't
    // re-allocate a new buffer in every call to `readNextGroup`
    private final byte[] buf;

    // position of next element within group to yield. If -1 or 4, means we need to read a new group
    // if -1, means we never read a group
    private int posInGroup;

    public GroupVarintDecoder(InputStream inputStream)
    {
        this.inputStream = inputStream;
        this.curGroup = new int[4];
        this.buf = new byte[4];

        this.posInGroup = -1;
    }

    /** Resets the state of the decoder, allowing treating the current position within the input stream as a new group.
     *
     * Note that while it's possible to reset the decoder in the middle of a group, any numbers which weren't read
     * will be lost - it's the caller's responsibility to ensure all numbers in the group(which aren't 0)
     * were exhausted beforehand.
     */
    @Override
    public void reset() throws IOException {
        this.posInGroup = -1;
    }

    // Tries to read a group of numbers, returns whether the read succeeded.
    private boolean readNextGroup() throws IOException {

        int bytesRead = inputStream.readNBytes(buf, 0, 1);
        if (bytesRead == 0) {
            return false;
        }
        assert bytesRead == 1;

        byte[] lengths = MASK_TO_LENGTHS[buf[0] & 0xff];

        for (int i=0; i < 4; ++i) {
            bytesRead = inputStream.readNBytes(buf, 0, lengths[i]);
            assert bytesRead == lengths[i];

            int curNumber = 0;
            for (int byteIx=0; byteIx < lengths[i]; ++byteIx)
            {
                curNumber = (curNumber << 8) + (buf[byteIx] & 0xff);
            }

            curGroup[i] = curNumber;

        }

        posInGroup = 0;
        return true;
    }

    /**
     * Reads an integer encoded via group varint encoding. Unlike the usual contract of InputStream.read, returned numbers
     * might utilize 31 bits.
     * @return A non negative integer, or -1 if there's no more input to read.
     * @throws IOException In case the backing stream throws
     */
    @Override
    public int read() throws IOException {
        if (posInGroup == -1 || posInGroup == 4) {
            // If we cant read the next group, it indicates we've finished
            if (!readNextGroup()) {
                return -1;
            }
        }

        int value = curGroup[posInGroup];
        posInGroup++;

        return value;
    }

    // Maps a length byte to the byte-lengths of the group
    final static byte[][] MASK_TO_LENGTHS = new byte[][] {
            {1, 1, 1, 1},
            {1, 1, 1, 2},
            {1, 1, 1, 3},
            {1, 1, 1, 4},
            {1, 1, 2, 1},
            {1, 1, 2, 2},
            {1, 1, 2, 3},
            {1, 1, 2, 4},
            {1, 1, 3, 1},
            {1, 1, 3, 2},
            {1, 1, 3, 3},
            {1, 1, 3, 4},
            {1, 1, 4, 1},
            {1, 1, 4, 2},
            {1, 1, 4, 3},
            {1, 1, 4, 4},
            {1, 2, 1, 1},
            {1, 2, 1, 2},
            {1, 2, 1, 3},
            {1, 2, 1, 4},
            {1, 2, 2, 1},
            {1, 2, 2, 2},
            {1, 2, 2, 3},
            {1, 2, 2, 4},
            {1, 2, 3, 1},
            {1, 2, 3, 2},
            {1, 2, 3, 3},
            {1, 2, 3, 4},
            {1, 2, 4, 1},
            {1, 2, 4, 2},
            {1, 2, 4, 3},
            {1, 2, 4, 4},
            {1, 3, 1, 1},
            {1, 3, 1, 2},
            {1, 3, 1, 3},
            {1, 3, 1, 4},
            {1, 3, 2, 1},
            {1, 3, 2, 2},
            {1, 3, 2, 3},
            {1, 3, 2, 4},
            {1, 3, 3, 1},
            {1, 3, 3, 2},
            {1, 3, 3, 3},
            {1, 3, 3, 4},
            {1, 3, 4, 1},
            {1, 3, 4, 2},
            {1, 3, 4, 3},
            {1, 3, 4, 4},
            {1, 4, 1, 1},
            {1, 4, 1, 2},
            {1, 4, 1, 3},
            {1, 4, 1, 4},
            {1, 4, 2, 1},
            {1, 4, 2, 2},
            {1, 4, 2, 3},
            {1, 4, 2, 4},
            {1, 4, 3, 1},
            {1, 4, 3, 2},
            {1, 4, 3, 3},
            {1, 4, 3, 4},
            {1, 4, 4, 1},
            {1, 4, 4, 2},
            {1, 4, 4, 3},
            {1, 4, 4, 4},
            {2, 1, 1, 1},
            {2, 1, 1, 2},
            {2, 1, 1, 3},
            {2, 1, 1, 4},
            {2, 1, 2, 1},
            {2, 1, 2, 2},
            {2, 1, 2, 3},
            {2, 1, 2, 4},
            {2, 1, 3, 1},
            {2, 1, 3, 2},
            {2, 1, 3, 3},
            {2, 1, 3, 4},
            {2, 1, 4, 1},
            {2, 1, 4, 2},
            {2, 1, 4, 3},
            {2, 1, 4, 4},
            {2, 2, 1, 1},
            {2, 2, 1, 2},
            {2, 2, 1, 3},
            {2, 2, 1, 4},
            {2, 2, 2, 1},
            {2, 2, 2, 2},
            {2, 2, 2, 3},
            {2, 2, 2, 4},
            {2, 2, 3, 1},
            {2, 2, 3, 2},
            {2, 2, 3, 3},
            {2, 2, 3, 4},
            {2, 2, 4, 1},
            {2, 2, 4, 2},
            {2, 2, 4, 3},
            {2, 2, 4, 4},
            {2, 3, 1, 1},
            {2, 3, 1, 2},
            {2, 3, 1, 3},
            {2, 3, 1, 4},
            {2, 3, 2, 1},
            {2, 3, 2, 2},
            {2, 3, 2, 3},
            {2, 3, 2, 4},
            {2, 3, 3, 1},
            {2, 3, 3, 2},
            {2, 3, 3, 3},
            {2, 3, 3, 4},
            {2, 3, 4, 1},
            {2, 3, 4, 2},
            {2, 3, 4, 3},
            {2, 3, 4, 4},
            {2, 4, 1, 1},
            {2, 4, 1, 2},
            {2, 4, 1, 3},
            {2, 4, 1, 4},
            {2, 4, 2, 1},
            {2, 4, 2, 2},
            {2, 4, 2, 3},
            {2, 4, 2, 4},
            {2, 4, 3, 1},
            {2, 4, 3, 2},
            {2, 4, 3, 3},
            {2, 4, 3, 4},
            {2, 4, 4, 1},
            {2, 4, 4, 2},
            {2, 4, 4, 3},
            {2, 4, 4, 4},
            {3, 1, 1, 1},
            {3, 1, 1, 2},
            {3, 1, 1, 3},
            {3, 1, 1, 4},
            {3, 1, 2, 1},
            {3, 1, 2, 2},
            {3, 1, 2, 3},
            {3, 1, 2, 4},
            {3, 1, 3, 1},
            {3, 1, 3, 2},
            {3, 1, 3, 3},
            {3, 1, 3, 4},
            {3, 1, 4, 1},
            {3, 1, 4, 2},
            {3, 1, 4, 3},
            {3, 1, 4, 4},
            {3, 2, 1, 1},
            {3, 2, 1, 2},
            {3, 2, 1, 3},
            {3, 2, 1, 4},
            {3, 2, 2, 1},
            {3, 2, 2, 2},
            {3, 2, 2, 3},
            {3, 2, 2, 4},
            {3, 2, 3, 1},
            {3, 2, 3, 2},
            {3, 2, 3, 3},
            {3, 2, 3, 4},
            {3, 2, 4, 1},
            {3, 2, 4, 2},
            {3, 2, 4, 3},
            {3, 2, 4, 4},
            {3, 3, 1, 1},
            {3, 3, 1, 2},
            {3, 3, 1, 3},
            {3, 3, 1, 4},
            {3, 3, 2, 1},
            {3, 3, 2, 2},
            {3, 3, 2, 3},
            {3, 3, 2, 4},
            {3, 3, 3, 1},
            {3, 3, 3, 2},
            {3, 3, 3, 3},
            {3, 3, 3, 4},
            {3, 3, 4, 1},
            {3, 3, 4, 2},
            {3, 3, 4, 3},
            {3, 3, 4, 4},
            {3, 4, 1, 1},
            {3, 4, 1, 2},
            {3, 4, 1, 3},
            {3, 4, 1, 4},
            {3, 4, 2, 1},
            {3, 4, 2, 2},
            {3, 4, 2, 3},
            {3, 4, 2, 4},
            {3, 4, 3, 1},
            {3, 4, 3, 2},
            {3, 4, 3, 3},
            {3, 4, 3, 4},
            {3, 4, 4, 1},
            {3, 4, 4, 2},
            {3, 4, 4, 3},
            {3, 4, 4, 4},
            {4, 1, 1, 1},
            {4, 1, 1, 2},
            {4, 1, 1, 3},
            {4, 1, 1, 4},
            {4, 1, 2, 1},
            {4, 1, 2, 2},
            {4, 1, 2, 3},
            {4, 1, 2, 4},
            {4, 1, 3, 1},
            {4, 1, 3, 2},
            {4, 1, 3, 3},
            {4, 1, 3, 4},
            {4, 1, 4, 1},
            {4, 1, 4, 2},
            {4, 1, 4, 3},
            {4, 1, 4, 4},
            {4, 2, 1, 1},
            {4, 2, 1, 2},
            {4, 2, 1, 3},
            {4, 2, 1, 4},
            {4, 2, 2, 1},
            {4, 2, 2, 2},
            {4, 2, 2, 3},
            {4, 2, 2, 4},
            {4, 2, 3, 1},
            {4, 2, 3, 2},
            {4, 2, 3, 3},
            {4, 2, 3, 4},
            {4, 2, 4, 1},
            {4, 2, 4, 2},
            {4, 2, 4, 3},
            {4, 2, 4, 4},
            {4, 3, 1, 1},
            {4, 3, 1, 2},
            {4, 3, 1, 3},
            {4, 3, 1, 4},
            {4, 3, 2, 1},
            {4, 3, 2, 2},
            {4, 3, 2, 3},
            {4, 3, 2, 4},
            {4, 3, 3, 1},
            {4, 3, 3, 2},
            {4, 3, 3, 3},
            {4, 3, 3, 4},
            {4, 3, 4, 1},
            {4, 3, 4, 2},
            {4, 3, 4, 3},
            {4, 3, 4, 4},
            {4, 4, 1, 1},
            {4, 4, 1, 2},
            {4, 4, 1, 3},
            {4, 4, 1, 4},
            {4, 4, 2, 1},
            {4, 4, 2, 2},
            {4, 4, 2, 3},
            {4, 4, 2, 4},
            {4, 4, 3, 1},
            {4, 4, 3, 2},
            {4, 4, 3, 3},
            {4, 4, 3, 4},
            {4, 4, 4, 1},
            {4, 4, 4, 2},
            {4, 4, 4, 3},
            {4, 4, 4, 4},
    };

}
