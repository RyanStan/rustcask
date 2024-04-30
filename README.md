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

** I should call this out in my implementation, and mention this other "blazingly fast key store":**
The current concurrency model is that each open creates a Bitcask handle with a set of readers and a writer.
The user of the library is responsible for spawning threads to execute reads and writes. However, the library is limited in that 
we share a set of BufReader<File> instances for a given rustcask instance. Therefore, reads on the same data file will be serialized because the
"deserialize_from' function requires a mutable reference to the reader. To fix this, we could make sure that each calling thread 
has its own set of BufReaders for each file.

The "blazingly fast key store" works as follows:
- Main thread calls open on Bitcask. This returns a Bitcask instance. The open function creates one writer, and creates n readers, 
 where n is the number of cores. Each reader gets its own LRU cache of File handles (called LogReaders). It sticks all of this into "the handle"
- The worker threads call "get handle" on the bitcask instance. This handle contains the thread safe queue of readers, the writer, and a wrapper around shared state.
- The worker thread calls get, put, delete, merge, on this handle. If the worker calls get, then one of the N readers performs the necessary read of the data file.
    (These readers are shared across all threads via the handle, but each reader has a file handle on each data file)

In my implementation, I want to leave the choice of parallelism up to the user, so I think I'd just allow every thread to have it's own set of reader file handles,
but to share the same writer. 

### Performance testing
I should implement a way to inject configurations, and then use the Criterion crate to test different configurations.

### Other design decisions
- memory mapped vs standard i/o. memory mapped = virtual memory system takes care of buffering.
- We can still buffer with memory mapped i/o, if we need to read larger chunks.
    I'll just use the buffered reader for now. It should work nicely because we avoid random seeks when reading or writing

- max size of data files. Default in rustcask is 2 GiB

- name of files: generation number.rustcask.data

- We create a hint file when closing one of the active data files.

- API accepts Vector<u8> instead of references followed by cloning. Up to caller to decide to create memory clone if need be.

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

### Post implementation todos
- Clean up error handling
- Implement logging with tracing crate
- Clean up code to use more abstractions. I can use this implementation as a reference: https://github.com/ltungv/bitcask/blob/master/src/storage/bitcask/log.rs#L19.
    E.g. I like how we built his own type that implements writer, which does extra things. 

### Memory mapping vs. buffered reads
I should do a performance experiment to see which is faster. Memory mappings seems like it might be more efficient for random reads (less read syscalls), at the expense
of increasing the memory usage of the process.

TODO: It would be very interesting to ping the "blazingly fast bitcask in Rust" person, and see why they chose to use memory mapped io instead of just buffering. 
It's interesting that they just used buffered reads while building the keydir, but use memory mapped files for normal operations. 
Once I have an answer from this person, it would be very interesting to touch on this topic of memory mapped vs. buffered reads in a blog post. Of course I need some performance tests set up.

If you used memory mapped files while building the keystore, you would map all the data files into memory! Not ideal. But over course of normal operation, you can just map in the most frequently accessed sections.


### Journal
4/6: Implemented remove and fixed bug in get that caused stack overflows. 
Next up: I should clean up logging (tracing crate) and errors (anyhow and thiserror). 
Then implement merge/compaction and hint files. Then clean up the threading stuff - each thread shold have a way to get its own set of readers, but still share the same writer.
Update: I'm going to re-write so the threading stuff works. Then go from there.

Leaving concurrency up to the caller is probably not what we want. They may have to make extra API calls,
or synchronize their own write calls ... It's easier to let them just specify the concurrency of the rustcask instance
and then handle it for them.

4/7: Finished concurrency re-write. And cleaned up remove. On to closing data files once they get a certain size.
    Then do logging, error, and benchmarks. Then hint files + merging. Then done!

4/10: Implement logic to close active data file if its too big

4/11: I think I implemented the data file rotation logic. I just need to write some tests for it next.

### Concurrency in LevelDB:
A database may only be opened by one process at a time. The leveldb implementation acquires a lock from the operating system to prevent misuse. Within a single process, the same leveldb::DB object may be safely shared by multiple concurrent threads. I.e., different threads may write into or fetch iterators or call Get on the same database without any external synchronization (the leveldb implementation will automatically do the required synchronization). However other objects (like Iterator and WriteBatch) may require external synchronization. If two threads share such an object, they must protect access to it using their own locking protocol. More details are available in the public header files.

And how I should handle it here:
Since each thread has to clone leveldb anyways, then they'll get cloned readers! And they can share the same writer

// TODO: move the builder back over.. since lots of shared methods.


### Performance
My ssd data sheet: https://www.mouser.com/datasheet/2/146/ssd_pro_6000p_brief-2474541.pdf
I'm hitting about hardware entitlement for sequential writes

How should I handle durability? Should I sync every write to disk? That kills performance.
Databases use group commit, and bundle writes to disk together.

See how rocksdb handles it: https://github.com/facebook/rocksdb/wiki/WAL-Performance.
TODO: I should see how bitcask handles it, but I think I'll avoid syncing, and mention the durability thing in the README.

Next up:
- error handling and logs
- hint files
- clean up readme
Done.


### Error handling
https://sabrinajewson.org/blog/errors
"Units of fallibility"....
Talk about how this inspired me to set up my error types the way I did, and include them as close to
the code that threw them as I could.

### Documentation
Create some documentation at the end of this

### Re-structuring
I have a lot of logic in my rustcask.rs file. Is there a way to split this up?
I should get rid of the rustcask.rs file.