% Copyright 2017-2018 Gregor Uhlenheuer
%
% Licensed under the Apache License, Version 2.0 (the "License");
% you may not use this file except in compliance with the License.
% You may obtain a copy of the License at
%
%     http://www.apache.org/licenses/LICENSE-2.0
%
% Unless required by applicable law or agreed to in writing, software
% distributed under the License is distributed on an "AS IS" BASIS,
% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
% See the License for the specific language governing permissions and
% limitations under the License.

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

-type metric_value() :: number() | null.

-type metric_tuple() :: {integer(), metric_value()}.

-record(udp_config, {
          port :: integer(),
          interval :: integer(),
          prune_after :: integer()
         }).

-type udp_config() :: #udp_config{}.

-record(protobuf_config, {
          port :: integer()
         }).

-type protobuf_config() :: #protobuf_config{}.

-record(tcp_config, {
          port :: integer()
         }).

-type tcp_config() :: #tcp_config{}.

-record(api_config, {
          port :: integer()
         }).

-type api_config() :: #api_config{}.

-record(listener_config, {
          listeners :: integer(),
          port :: integer(),
          supervisor :: atom(),
          child_name :: atom(),
          options=[] :: [atom() | tuple()]
         }).

-type listener_config() :: #listener_config{}.

-record(rate_limit_config, {
          creates_per_sec :: integer(),
          updates_per_sec :: integer()
         }).

-type rate_limit_config() :: #rate_limit_config{}.

-record(whisper_archive, {
          offset :: integer(),
          seconds :: integer(),
          points :: integer(),
          retention :: integer(),
          size :: integer()
         }).

-type whisper_archive() :: #whisper_archive{}.

-record(whisper_metadata, {
          aggregation :: aggregation(),
          retention :: integer(),
          xff :: float(),
          archives :: [#whisper_archive{}]
         }).

-type whisper_metadata() :: #whisper_metadata{}.

-record(retention_definition, {
          raw :: string(),
          seconds :: integer(),
          points :: integer()
         }).

-type retention_definition() :: #retention_definition{}.

-record(storage_definition, {
          name :: nonempty_string(),
          pattern :: tuple() | undefined,
          retentions :: [retention_definition(),...]
         }).

-type storage_definition() :: #storage_definition{}.

-record(aggregation_definition, {
          name :: nonempty_string(),
          pattern :: tuple() | undefined,
          aggregation :: aggregation(),
          factor :: float()
         }).

-type aggregation_definition() :: #aggregation_definition{}.

-record(series, {
          target :: binary() | undefined,
          values :: [metric_tuple()],
          step :: integer(),
          start :: integer(),
          until :: integer(),
          aggregation :: aggregation()
         }).

-type series() :: #series{}.

-record(metric_pattern, {
          name :: nonempty_string(),
          pattern :: tuple()
         }).

-type metric_pattern() :: #metric_pattern{}.

-record(metric_filters, {
          whitelist=[] :: [#metric_pattern{}],
          blacklist=[] :: [#metric_pattern{}]
         }).

-type metric_filters() :: #metric_filters{}.


-type node_status() :: connected | disconnected | me.

-record(node_info, {
          node :: node(),
          pid :: pid() | undefined,
          state=disconnected :: node_status(),
          last_seen=0 :: integer()
         }).

-type node_info() :: #node_info{}.
