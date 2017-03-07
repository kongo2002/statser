-module(statser_whisper).

-export([read_metadata/1,
         aggregation_type/1,
         aggregation_value/1]).


% 16 bytes = 4 metadata fields x 4 bytes
-define(METADATA_HEADER_SIZE, 16).

% 12 bytes = 3 archive header fields x 4 bytes
-define(METADATA_ARCHIVE_HEADER_SIZE, 12).

% 12 bytes = 4 bytes (timestamp) + 8 bytes (value)
-define(POINT_SIZE, 12).


-record(metadata, {aggregation, retention, xff, archives}).

-record(archive_header, {offset, seconds, points, retention, size}).


aggregation_type(1) -> average;
aggregation_type(2) -> sum;
aggregation_type(3) -> last;
aggregation_type(4) -> max;
aggregation_type(5) -> min;
aggregation_type(6) -> average_zero.


aggregation_value(average) -> 1;
aggregation_value(sum) -> 2;
aggregation_value(last) -> 3;
aggregation_value(max) -> 4;
aggregation_value(min) -> 5;
aggregation_value(average_zero) -> 6.


read_metadata(File) ->
    {ok, IO} = file:open(File, [read, binary]),
    try read_metadata_inner(IO)
        after file:close(IO)
    end.

read_metadata_inner(IO) ->
    {ok, Header} = file:read(IO, ?METADATA_HEADER_SIZE),
    case read_header(Header) of
        {ok, AggType, MaxRet, XFF, Archives} ->
            case read_archive_info(IO, Archives) of
                error -> error;
                As ->
                    Metadata = #metadata{aggregation=AggType,
                                        retention=MaxRet,
                                        xff=XFF,
                                        archives=As},
                    {ok, Metadata}
            end;
        error -> error
    end.

read_archive_info(IO, Archives) ->
    case read_archive_info(IO, [], Archives) of
        error -> error;
        As -> lists:reverse(As)
    end.

read_archive_info(_IO, As, 0) -> As;
read_archive_info(IO, As, Archives) ->
    case file:read(IO, ?METADATA_ARCHIVE_HEADER_SIZE) of
        {ok, <<Offset:32/integer-unsigned-big, Secs:32/integer-unsigned-big, Points:32/integer-unsigned-big>>} ->
            Archive = #archive_header{offset=Offset,
                                     seconds=Secs,
                                     points=Points,
                                     retention=Secs*Points,
                                     size=Points*?POINT_SIZE},
            read_archive_info(IO, [Archive | As], Archives - 1);
        _Error -> error
    end.

read_header(<<AggType:32/unsigned-integer-big,
              MaxRetention:32/unsigned-integer-big,
              XFF:32/float-big,
              NumArchives:32/integer-unsigned-big>>) ->
    {ok, aggregation_type(AggType), MaxRetention, XFF, NumArchives};
read_header(_) -> error.


write_header(AggType, MaxRetention, XFF, NumArchives) ->
    % aggregation type: 4 bytes, unsigned
    % max retention:    4 bytes, unsigned
    % x-files-factor:   4 bytes, float
    % archives:         4 bytes, unsigned
    AggValue = aggregation_value(AggType),
    <<AggValue:32/unsigned-integer-big,
      MaxRetention:32/unsigned-integer-big,
      XFF:32/float-big,
      NumArchives:32/integer-unsigned-big>>.
