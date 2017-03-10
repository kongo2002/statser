-module(statser_whisper).

-include_lib("eunit/include/eunit.hrl").


-export([read_metadata/1,
         aggregation_type/1,
         aggregation_value/1,
         update_point/3]).


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


make_archive_header(Offset, Seconds, Points) ->
    #archive_header{offset=Offset,
                   seconds=Seconds,
                   points=Points,
                   retention=Seconds * Points,
                   size=Points * ?POINT_SIZE}.


read_archive_info(IO, Archives) ->
    ByOffset = fun(A, B) -> A#archive_header.offset =< B#archive_header.offset end,
    case read_archive_info(IO, [], Archives) of
        error -> error;
        As -> lists:sort(ByOffset, As)
    end.

read_archive_info(_IO, As, 0) -> As;
read_archive_info(IO, As, Archives) ->
    case file:read(IO, ?METADATA_ARCHIVE_HEADER_SIZE) of
        {ok, <<Offset:32/integer-unsigned-big, Secs:32/integer-unsigned-big, Points:32/integer-unsigned-big>>} ->
            Archive = make_archive_header(Offset, Secs, Points),
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


highest_precision_archive(TimeDiff, [#archive_header{retention=Ret} | As]) when Ret < TimeDiff ->
    highest_precision_archive(TimeDiff, As);
highest_precision_archive(_TimeDiff, [A | As]) -> {A, As};
highest_precision_archive(_TimeDiff, []) -> error.


data_point(#archive_header{seconds=S}, TimeStamp, Value) ->
    Interval = TimeStamp - (TimeStamp rem S),
    <<Interval:32/integer-unsigned-big, Value:64/float-big>>.


read_point(IO) ->
    case file:read(IO, ?POINT_SIZE) of
        {ok, <<Interval:32/integer-unsigned-big, Value:64/float-big>>} ->
            {Interval, Value};
        Unexpected ->
            lager:error("read unexpected data point: ~p", [Unexpected]),
            error
    end.


get_data_point_offset(Archive, Interval, 0) ->
    Archive#archive_header.offset;
get_data_point_offset(Archive, Interval, BaseInterval) ->
    Distance = Interval - BaseInterval,
    PointDistance = Distance div Archive#archive_header.seconds,
    ByteDistance = PointDistance * ?POINT_SIZE,
    Archive#archive_header.offset + (ByteDistance rem Archive#archive_header.size).


update_point(File, Value, TimeStamp) ->
    {ok, IO} = file:open(File, [write, read, binary]),
    try do_update(IO, Value, TimeStamp)
        after file:close(IO)
    end.


do_update(IO, Value, TimeStamp) ->
    {ok, Metadata} = read_metadata_inner(IO),
    write_point(IO, Metadata, Value, TimeStamp).


write_point(IO, Header, Value, TimeStamp) ->
    Now = erlang:system_time(second),
    TimeDiff = Now - TimeStamp,
    MaxRetention = Header#metadata.retention,
    if
        TimeDiff >= MaxRetention, TimeDiff < 0 ->
            lager:error("timestamp ~p is not covered by any archive of ~p", [TimeStamp, Header]),
            error;
        true ->
            % find highest precision and lower archives to update
            {Archive, LowerArchives} = highest_precision_archive(TimeDiff, Header#metadata.archives),

            % seek first data point
            {ok, _} = file:position(IO, {bof, Archive#archive_header.offset}),

            % read base data point
            {BaseInterval, _Value} = read_point(IO),
            Position = get_data_point_offset(Archive, TimeStamp, BaseInterval),

            % write data point based on initial data point
            {ok, _} = file:position(IO, {bof, Position}),
            file:write(IO, data_point(Archive, TimeStamp, Value))

            % TODO: update lower archives
    end.

%%
%% TESTS
%%

highest_precision_archive_test_() ->
    Archive = make_archive_header(28, 60, 1440),
    Archive2 = make_archive_header(28, 300, 1000),

    [?_assert(highest_precision_archive(100, []) =:= error),
     ?_assert(highest_precision_archive(100, [Archive]) =:= {Archive, []}),
     ?_assert(highest_precision_archive(86400, [Archive]) =:= {Archive, []}),
     ?_assert(highest_precision_archive(100, [Archive, Archive2]) =:= {Archive, [Archive2]}),
     ?_assert(highest_precision_archive(86400, [Archive, Archive2]) =:= {Archive, [Archive2]}),
     ?_assert(highest_precision_archive(86401, [Archive, Archive2]) =:= {Archive2, []}),
     ?_assert(highest_precision_archive(300 * 1000 + 1, [Archive, Archive2]) =:= error)].

get_data_point_offset_test_() ->
    Now = erlang:system_time(second),
    Offset = 28,
    Archive = make_archive_header(Offset, 60, 1440),

    [?_assert(get_data_point_offset(Archive, Now, 0) =:= Offset),
     ?_assert(get_data_point_offset(Archive, Now+60, Now) =:= Offset + ?POINT_SIZE),
     ?_assert(get_data_point_offset(Archive, Now+119, Now) =:= Offset + ?POINT_SIZE),
     ?_assert(get_data_point_offset(Archive, Now+120, Now) =:= Offset + ?POINT_SIZE + ?POINT_SIZE)].
