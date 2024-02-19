# Rustcask
A Bitcask implementation in Rust: log-based key-value store.

### Limitations
- Only a single process (possibly multi-threaded) can access a particular database at a time.

### Serialization format
- Describe the serialization format that I chose

### Concurrency
I should draw up a diagram about how I decide to handle concurrency similar to the diagrams draw in this blog post:
    https://www.tunglevo.com/note/build-a-blazingly-fast-key-value-store-with-rust/#coarse-grained-locking.

A simple read write mutex around the context might be enough to start with. Then I can upgrade to allowing concurrent readers and writers.
I'll have to run some tests to assert consistency guarantees.

Reads and writes will be linearizable since keydir + file appends are atomic. 

Concurrent reads and writes --> the reads might not pick up the writes. 

We won't support any concept of transactions.

### Other design decisions
- memory mapped vs standard i/o. memory mapped = virtual memory system takes care of buffering.
- We can still buffer with memory mapped i/o, if we need to read larger chunks.
    I'll just use the buffered reader for now. It should work nicely because we avoid random seeks when reading or writing

- max size of data files. Default in rustcask is 2 GiB

- name of files: generation number.rustcask.data

- We create a hint file when closing one of the active data files.

### Merging
When merge happens, just read over all files and create a new data file + hint file.

### Performance
I should have a python package or script that executes benchmarks against the key-value store and plots results.
It would be very cool to compare different benchmarks when I change how the implementation works. For example,
performance of keeping file handles open for reads, vs. having each reader open then close the file.

- Experiment with buffered I/O chunk size


### My notes during implementation
- We can keep the log files open by the readers
- The active lock file has to be opened with a write lock, then closed once set is done. 
    We'll want to get the open write lock when performing the set operation.
    We don't want to lock on the open, because we'd like multiple concurrent writers.
    We just need to make sure that things are linearlizable (append only, this will be the case).
    --> we can just use internal locks since the threads share memory to ensure only one writes at a time.


- We keep an open reader for each data file... to save access time by avoiding file opens, right?

- Nice implementation that I can look to: https://github.com/ltungv/bitcask/blob/master/src/storage/bitcask.rs.



- this is where the real bitcask shares keydir among processes (or at least allocates
it and mentions it will be shareable) - https://github.com/basho/bitcask/blob/d84c8d913713da8f02403431217405f84ee1ba22/c_src/bitcask_nifs.c#L424-L429


- We warrant that these two steps are performed atomically and in order. By making writes on disk always happens before any modifications are made on KeyDir, we can ensure that every entry in KeyDir points to a valid DataFileEntry. Under this guarantee, we can be confident that LogReader won’t misbehave 


Should you read all records in the log into memory at once and then replay them into your map type; or should you read them one at a time while replaying them into your map? Should you read into a buffer before deserializing or deserialize from a file stream? Think about the memory usage of your approach. Think about the way reading from I/O streams interacts with the kernel.
--> idk??


- We'll have to read data files + hints in order. Just read the hint file if we can. 
- Only write the hint file after we close the file. 

Why don't we just merge everytime we close a file? I think you lose something if you don't just append.
You could crash while rewriting file then you lose everything. Plus rewriting an entire file is bad for disk access. Lots of seeks!!! Would be cool to benchmark though.

### Some nice notes from chatgpt on how to think about concurrency, and why we can't have concurrent writers or concurrent readers/writers
    Concurrent Writes: If multiple writers attempt to modify the same file simultaneously, it can lead to data corruption or loss. To mitigate this, you can use file locks or synchronization primitives (such as mutexes or semaphores) to ensure that only one writer has access to the file at a time.

    Read-Write Races: When a writer modifies a file while a reader is accessing it, the reader may observe inconsistent or partially updated data. To address this, you can use file locks or read-write locks to coordinate access between readers and writers, ensuring that writers have exclusive access to the file while they are modifying it.

    File System Metadata Races: Changes to file system metadata (such as file size, timestamps, or permissions) can occur concurrently with file reads or writes, leading to inconsistent or unexpected behavior. While file systems typically provide mechanisms to handle metadata updates atomically, you should be aware of potential race conditions and design your application to handle them gracefully.

    Partial Writes: If a writer performs a partial write operation (e.g., due to an error or interruption), concurrent readers may observe incomplete or corrupted data. To mitigate this, you can use techniques such as atomic file updates or write-ahead logging to ensure that write operations are either fully completed or rolled back in case of failure.

    Concurrency Control: Implementing proper concurrency control mechanisms, such as locking, atomic operations, or transactional semantics, can help prevent data races and ensure the consistency and integrity of file operations in a concurrent environment.

### ChatGPT on the optimal data file size
The size of data files in Bitcask, or any similar key-value store, is a crucial consideration that can impact performance, resource utilization, and overall system efficiency. Determining the optimal file size involves balancing various factors, and there is no one-size-fits-all answer. Here are some considerations regarding data file size in Bitcask:
Pros and Cons of Larger Files:
Pros:

    Reduced Metadata Overhead: With larger data files, there are fewer files overall, which can reduce metadata overhead on the file system. This can result in faster file system operations and improved performance, especially for file-based operations such as file creation, deletion, and traversal.

    Sequential Access: Larger data files facilitate sequential access patterns, which can be more efficient for both read and write operations. Sequential access minimizes disk seek times and maximizes data throughput, particularly on spinning disks.

    Reduced File Fragmentation: Larger data files are less prone to fragmentation compared to smaller files. Fragmentation occurs when files are split into smaller, non-contiguous blocks on disk, leading to increased disk I/O overhead. By reducing file fragmentation, larger files can improve disk performance and longevity.

Cons:

    Increased Memory Usage: Larger data files require more memory for caching and buffering, both within the Bitcask process and by the operating system's file cache. This can lead to higher memory usage and increased pressure on system resources, particularly in memory-constrained environments.

    Slower Compaction: Compaction processes, which merge and rewrite data files to reclaim disk space, can be slower for larger files. Compacting large files involves copying and rewriting larger amounts of data, which can increase the duration of compaction operations and potentially impact system responsiveness.

    Longer Recovery Times: In the event of a system failure or crash, recovering data from larger files may take longer compared to smaller files. Recovery processes may need to scan and process larger volumes of data, leading to increased downtime and slower system recovery.