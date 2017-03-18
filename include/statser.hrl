
% 16 bytes = 4 metadata fields x 4 bytes
-define(METADATA_HEADER_SIZE, 16).

% 12 bytes = 3 archive header fields x 4 bytes
-define(METADATA_ARCHIVE_HEADER_SIZE, 12).

% 12 bytes = 4 bytes (timestamp) + 8 bytes (value)
-define(POINT_SIZE, 12).

% default write chunk size of 4kb
-define(WRITE_CHUNK_SIZE, 4096).


-type aggregation() :: average | sum | last | max | min | average_zero.

-record(archive_header, {
          offset :: integer(),
          seconds :: integer(),
          points :: integer(),
          retention :: integer(),
          size :: integer()}).

-record(metadata, {
          aggregation :: aggregation(),
          retention :: integer(),
          xff :: float(),
          archives :: [#archive_header{}]
         }).
