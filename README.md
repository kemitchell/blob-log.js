## File Format

blob-log log files consist of a big-endian, unsigned, 32-bit integer
encoding the sequence number of the first blob in the file, followed
by one or more blob records.  Each blob record consists of:

1. the number of bytes in the blob, as a big-endian, unsigned,
   32-bit integer

2. a CRC-32 error-correcting code for the blob, as a big-endian,
   unsigned, 32-bit integer

3. the blob's bytes
