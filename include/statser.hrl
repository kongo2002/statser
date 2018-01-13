
% 16 bytes = 4 metadata fields x 4 bytes
-define(METADATA_HEADER_SIZE, 16).

% 12 bytes = 3 archive header fields x 4 bytes
-define(METADATA_ARCHIVE_HEADER_SIZE, 12).

% 12 bytes = 4 bytes (timestamp) + 8 bytes (value)
-define(POINT_SIZE, 12).

% default write chunk size of 4kb
-define(WRITE_CHUNK_SIZE, 4096).

-define(MILLIS_PER_SEC, 1000).


-type aggregation() :: average | sum | last | max | min | average_zero.

-type duration_unit() :: default | seconds | minutes | hours | days | weeks | years.

-record(udp_config, {
          port :: integer(),
          interval :: integer(),
          prune_after :: integer()
         }).

-record(protobuf_config, {
          port :: integer()
         }).

-record(listener_config, {
          listeners :: integer(),
          port :: integer(),
          supervisor :: atom(),
          child_name :: atom()
         }).

-record(rate_limit_config, {
          creates_per_sec :: integer(),
          updates_per_sec :: integer()
         }).

-record(whisper_archive, {
          offset :: integer(),
          seconds :: integer(),
          points :: integer(),
          retention :: integer(),
          size :: integer()
         }).

-record(whisper_metadata, {
          aggregation :: aggregation(),
          retention :: integer(),
          xff :: float(),
          archives :: [#whisper_archive{}]
         }).

-record(retention_definition, {
          raw :: string(),
          seconds :: integer(),
          points :: integer()
         }).

-record(storage_definition, {
          name :: nonempty_string(),
          pattern :: tuple() | undefined,
          retentions :: [#retention_definition{},...]
         }).

-record(aggregation_definition, {
          name :: nonempty_string(),
          pattern :: tuple() | undefined,
          aggregation :: aggregation(),
          factor :: float()
         }).

-record(series, {
          target :: nonempty_string(),
          values :: [{integer(), number()}],
          step :: integer(),
          start :: integer(),
          until :: integer(),
          aggregation :: aggregation()
         }).

-record(metric_pattern, {
          name :: nonempty_string(),
          pattern :: tuple()
         }).

-record(metric_filters, {
          whitelist=[] :: [#metric_pattern{}],
          blacklist=[] :: [#metric_pattern{}]
         }).
