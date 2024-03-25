use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};

/// A wrapper a round `BufReader` that keeps track of the current position within the inner reader.
/// This code is adapted from https://github.com/ltungv/bitcask/blob/master/src/storage/bitcask/bufio.rs.
/// 
/// If you're using a BufReaderWithPos and you want to get your current offset in the underlying
/// reader, then you have to use the seek method with a relative offset of zero.
/// However, on BufReaders, the seek method has the side effect of emptying the buffer.
/// That's why we need this wrapper class which tracks the read position.
pub struct BufReaderWithPos<R> 
where
    R: Read + Seek
{
    pos: u64,
    reader: BufReader<R>
}

impl<R> BufReaderWithPos<R>
where
    R: Read + Seek
{
    pub fn new(mut inner_reader: R) -> io::Result<Self> {
        let pos = inner_reader.stream_position()?;
        let reader = BufReader::new(inner_reader);
        Ok(BufReaderWithPos { pos, reader })
    }

    /// Return the current seek position within the underlying reader.
    pub fn pos(&self) -> u64 {
        self.pos
    }
}

impl<R> Read for BufReaderWithPos<R> 
where 
    R: Read + Seek
{
    fn read(&mut self, buf: &mut [u8]) -> Result<usize, std::io::Error> {
        self.reader.read(buf).map(|bytes_read| {
            self.pos += bytes_read as u64;
            bytes_read
        })
    }
}

impl<R> Seek for BufReaderWithPos<R> 
where 
    R: Read + Seek,
{
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        let offset = self.seek(pos)?;
        self.pos = offset;
        Ok(offset)
    }
}

