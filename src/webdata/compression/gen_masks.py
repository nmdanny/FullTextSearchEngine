
def gen_lengths(mask):
    assert mask < 2 ** 8

    lengths = [0] * 4
    # add 1 since lengths are between 1 and 4
    lengths[0] = ((mask & 0b11000000) >> 6) + 1
    lengths[1] = ((mask & 0b00110000) >> 4) + 1
    lengths[2] = ((mask & 0b00001100) >> 2) + 1
    lengths[3] =  (mask & 0b00000011) + 1

    return lengths


def main():
    print("final static byte MASK_TO_LENGTHS[][] = new byte[][] {")
    for mask in range(2 ** 8):
        lengths = gen_lengths(mask)
        lengths_str = ", ".join(str(length) for length in lengths)
        print(f"  {{{lengths_str}}},", end=None)
    print("};")


if __name__ == "__main__":
    main()