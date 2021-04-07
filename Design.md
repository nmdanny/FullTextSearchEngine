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

I will use the 'blocking' method for compressing the dictionary. While using the (k-1)-in-k method would've been more memory
efficient, due to time constraints I will not implement it. However, the strings file will be kept in a memory map as explained
below, which should improve memory usage.

## Use of memory mapping

All terms/tokens will be kept in a memory mapped file - this has several benefits:

- Easier to program: We don't need to manually handle persisting the strings, this is automatically done whenever we
  modify the file (assuming we eventually flush/close it). We can treat the file like an ordinary array. And if the number

- Ensures low memory usage even when the number of strings is very large, at the cost of disk IO.
  
- More efficient in contrast to usual Java IO: 
  - Reading/writing from a memory mapped file doesn't require costly sys-calls, and can utilize various kinds of caches
    more efficiently than if we had used standard IO operations. 
    
  - Separation of concerns - instead of manually deciding how to buffer data, we essentially let the OS handle this.
    On a system with a lot of RAM or a sufficiently small number of unique tokens, the OS would probably choose to load many(or even all)
    of the file's pages into disk, and in the opposite case, we'd simply encounter page faults more frequently.
  

## Object Storage

The non textual fields (productId, helpfulness and score) will be stored in a binary
file sorted by their docIds (same order as appearance as in the original text file). Since those fields are
of constant size, we can determine the offset of a given docId by calculation, allowing us to do O(1) indexing on
the file.


## Serialization

I will not use Java's object based serialization, and rather use `DataOutput/InputStream` - this
is because serializing objects adds metadata to every object instance, which is costly. 