# Full Text Search engine

A full text search engine implemented as part of HUJI's Web Information Retrieval course.

The engine currently supports a specific dataset - Amazon product review data taken from [here](https://nijianmo.github.io/amazon/index.html), using
a line-oriented data format (see the .txt files under `datasets` for an example)


The main classes of this library are:

- `webdata.IndexWriter`, for constructing the index given a dataset file

- `webdata.IndexReader` for querying the index

- `webdata.ReviewSearch` for performing various text search operations 

# Documentation

- Click [here](analysis/analysis.pdf) for an explanation and visualization of the index structure, as well as theoretical
  runtime analysis of index operations.

- Click [here](analysis/analysis2.pdf) for various benchmarks of index construction and querying.

- Click [here](analysis/analysis3.pdf) for an explanation of a custom product ranking function I've implemented for product search.

Most of the classes and methods were also documented, see below on how to create javadocs.

# Build instructions

Requires Java 11+ and Maven. 

- Type `mvn package` to compile, test and package this library, and generate docs.The resulting jars will be located at `target`.

  Documentation can be found at `target/apidocs/index.html`

  (Skip testing by adding `-Dmaven.test.skip=true`)