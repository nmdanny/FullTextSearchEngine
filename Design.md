# Design & Implementation Thoughts


## Inverted Index

### Structure

The inverted index is a binary file consisting of many posting lists in sequence,
each contains elements of the form `(docIdGap, frequency)`


### Encoding

For both kinds of numbers(gaps & frequencies), I will use group varint encoding.
It seems reasonably simple to implement, and strikes a decent balance between memory
efficiency and CPU efficiency. While bit-level encoding might be more memory efficient,
due to memory alignment, it is likely to be slower, and is also trickier to implement.

Note that we cannot do binary search when using group varint encoding, but it's not
an issue as we don't need this capability for a posting list.

### Implementation notes

- The encoder is implemented as an `OutputStream` while the encoder is implemented as an `InputStream`.
  This allows easily composing them with Java's file stream classes. 

- At first glance, group variant encoding requires the number of elements to be divisible by 4,
  which won't necessarily happen. We handle this by using the number 0 (encoded as `0x00` in group varint) as
  a sentinel value indicating that the stream has finished. This is OK because neither gaps nor frequencies
  can be 0. 
  
  Since our groups consist of at most 4 integers, and each term occurrence consists of 2 integers,
  we can either have no missing integers or exactly 2 missing integers. 

## Dictionary

I will use the 'blocking' method for compressing the dictionary. 

## Object Storage

The non textual fields (productId, helpfulness and score) will be stored in a binary
file sorted by their docIds (same order as appearance as in the original text file). Since those fields are
of constant size, we can determine the offset of a given docId by calculation, allowing us to do O(1) indexing on
the file.


## Serialization

I will not use Java's object based serialization, and rather use `DataOutput/InputStream` - this
is because serializing objects adds metadata to every object instance, which is costly. 