-module(statser_parser).
-export([parse/1,file/1]).
-define(p_anything,true).
-define(p_charclass,true).
-define(p_choose,true).
-define(p_label,true).
-define(p_not,true).
-define(p_one_or_more,true).
-define(p_optional,true).
-define(p_scan,true).
-define(p_seq,true).
-define(p_string,true).
-define(p_zero_or_more,true).



-spec file(file:name()) -> any().
file(Filename) -> case file:read_file(Filename) of {ok,Bin} -> parse(Bin); Err -> Err end.

-spec parse(binary() | list()) -> any().
parse(List) when is_list(List) -> parse(unicode:characters_to_binary(List));
parse(Input) when is_binary(Input) ->
  _ = setup_memo(),
  Result = case 'root'(Input,{{line,1},{column,1}}) of
             {AST, <<>>, _Index} -> AST;
             Any -> Any
           end,
  release_memo(), Result.

-spec 'root'(input(), index()) -> parse_result().
'root'(Input, Index) ->
  p(Input, Index, 'root', fun(I,D) -> (p_choose([fun 'template'/2, fun 'function'/2, fun 'paths'/2]))(I,D) end, fun(Node, Idx) ->transform('root', Node, Idx) end).

-spec 'template'(input(), index()) -> parse_result().
'template'(Input, Index) ->
  p(Input, Index, 'template', fun(I,D) -> (p_seq([p_string(<<"template(">>), fun 'expr'/2, fun 'spaces'/2, p_optional(p_seq([p_string(<<",">>), fun 'spaces'/2, fun 'arguments'/2])), fun 'spaces'/2, p_string(<<")">>)]))(I,D) end, fun(Node, _Idx) ->
case Node of
    [_, Expr, _, [], _, _] -> {template, Expr, []};
    [_, Expr, _, [_, _, Args], _, _] -> {template, Expr, Args}
end
 end).

-spec 'expr'(input(), index()) -> parse_result().
'expr'(Input, Index) ->
  p(Input, Index, 'expr', fun(I,D) -> (p_choose([fun 'function'/2, fun 'paths'/2]))(I,D) end, fun(Node, Idx) ->transform('expr', Node, Idx) end).

-spec 'function'(input(), index()) -> parse_result().
'function'(Input, Index) ->
  p(Input, Index, 'function', fun(I,D) -> (p_seq([fun 'alphanum'/2, p_string(<<"(">>), fun 'arguments'/2, p_string(<<")">>)]))(I,D) end, fun(Node, _Idx) ->
[Fctn, P1, Args, P2] = Node, {call, iolist_to_binary(Fctn), Args}
 end).

-spec 'arguments'(input(), index()) -> parse_result().
'arguments'(Input, Index) ->
  p(Input, Index, 'arguments', fun(I,D) -> (p_seq([p_label('head', fun 'argument'/2), p_label('tail', p_zero_or_more(p_seq([fun 'spaces'/2, fun 'comma'/2, fun 'spaces'/2, fun 'argument'/2, fun 'spaces'/2])))]))(I,D) end, fun(Node, _Idx) ->
Head = proplists:get_value(head, Node),
Rest = [lists:nth(4, I) || I <- proplists:get_value(tail, Node)],
[Head | Rest]
 end).

-spec 'argument'(input(), index()) -> parse_result().
'argument'(Input, Index) ->
  p(Input, Index, 'argument', fun(I,D) -> (p_choose([fun 'number'/2, fun 'bool'/2, fun 'singleq_string'/2, fun 'doubleq_string'/2, fun 'paths'/2]))(I,D) end, fun(Node, Idx) ->transform('argument', Node, Idx) end).

-spec 'paths'(input(), index()) -> parse_result().
'paths'(Input, Index) ->
  p(Input, Index, 'paths', fun(I,D) -> (fun 'path'/2)(I,D) end, fun(Node, _Idx) ->{paths, Node} end).

-spec 'path'(input(), index()) -> parse_result().
'path'(Input, Index) ->
  p(Input, Index, 'path', fun(I,D) -> (p_seq([p_label('head', fun 'path_elem'/2), p_label('tail', p_zero_or_more(p_seq([fun 'dot'/2, fun 'path_elem'/2])))]))(I,D) end, fun(Node, _Idx) ->
Head = proplists:get_value(head, Node),
Rest = [lists:nth(2, I) || I <- proplists:get_value(tail, Node)],
[Head | Rest]
 end).

-spec 'path_elem'(input(), index()) -> parse_result().
'path_elem'(Input, Index) ->
  p(Input, Index, 'path_elem', fun(I,D) -> (p_one_or_more(p_choose([fun 'wordchar'/2, p_seq([fun 'escape'/2, fun 'special'/2])])))(I,D) end, fun(Node, _Idx) ->iolist_to_binary(Node) end).

-spec 'wordchar'(input(), index()) -> parse_result().
'wordchar'(Input, Index) ->
  p(Input, Index, 'wordchar', fun(I,D) -> (p_charclass(<<"[a-zA-Z0-9*_-]">>))(I,D) end, fun(Node, Idx) ->transform('wordchar', Node, Idx) end).

-spec 'alphanum'(input(), index()) -> parse_result().
'alphanum'(Input, Index) ->
  p(Input, Index, 'alphanum', fun(I,D) -> (p_seq([p_charclass(<<"[a-zA-Z]">>), p_zero_or_more(p_charclass(<<"[a-zA-Z0-9]">>))]))(I,D) end, fun(Node, Idx) ->transform('alphanum', Node, Idx) end).

-spec 'special'(input(), index()) -> parse_result().
'special'(Input, Index) ->
  p(Input, Index, 'special', fun(I,D) -> (p_charclass(<<"[(){},=.\"]">>))(I,D) end, fun(Node, Idx) ->transform('special', Node, Idx) end).

-spec 'escape'(input(), index()) -> parse_result().
'escape'(Input, Index) ->
  p(Input, Index, 'escape', fun(I,D) -> (p_string(<<"\\">>))(I,D) end, fun(Node, Idx) ->transform('escape', Node, Idx) end).

-spec 'dot'(input(), index()) -> parse_result().
'dot'(Input, Index) ->
  p(Input, Index, 'dot', fun(I,D) -> (p_string(<<".">>))(I,D) end, fun(Node, Idx) ->transform('dot', Node, Idx) end).

-spec 'comma'(input(), index()) -> parse_result().
'comma'(Input, Index) ->
  p(Input, Index, 'comma', fun(I,D) -> (p_string(<<",">>))(I,D) end, fun(Node, Idx) ->transform('comma', Node, Idx) end).

-spec 'bool'(input(), index()) -> parse_result().
'bool'(Input, Index) ->
  p(Input, Index, 'bool', fun(I,D) -> (p_choose([fun 'true'/2, fun 'false'/2]))(I,D) end, fun(Node, Idx) ->transform('bool', Node, Idx) end).

-spec 'true'(input(), index()) -> parse_result().
'true'(Input, Index) ->
  p(Input, Index, 'true', fun(I,D) -> (p_string(<<"true">>))(I,D) end, fun(_Node, _Idx) ->true end).

-spec 'false'(input(), index()) -> parse_result().
'false'(Input, Index) ->
  p(Input, Index, 'false', fun(I,D) -> (p_string(<<"false">>))(I,D) end, fun(_Node, _Idx) ->false end).

-spec 'number'(input(), index()) -> parse_result().
'number'(Input, Index) ->
  p(Input, Index, 'number', fun(I,D) -> (p_choose([fun 'float'/2, fun 'int'/2]))(I,D) end, fun(Node, Idx) ->transform('number', Node, Idx) end).

-spec 'float'(input(), index()) -> parse_result().
'float'(Input, Index) ->
  p(Input, Index, 'float', fun(I,D) -> (p_seq([p_optional(p_string(<<"-">>)), p_one_or_more(fun 'digit'/2), p_string(<<".">>), p_one_or_more(fun 'digit'/2)]))(I,D) end, fun(Node, _Idx) ->list_to_float(binary_to_list(iolist_to_binary(Node))) end).

-spec 'int'(input(), index()) -> parse_result().
'int'(Input, Index) ->
  p(Input, Index, 'int', fun(I,D) -> (p_seq([p_optional(p_string(<<"-">>)), p_one_or_more(fun 'digit'/2)]))(I,D) end, fun(Node, _Idx) ->list_to_integer(binary_to_list(iolist_to_binary(Node))) end).

-spec 'digit'(input(), index()) -> parse_result().
'digit'(Input, Index) ->
  p(Input, Index, 'digit', fun(I,D) -> (p_charclass(<<"[0-9]">>))(I,D) end, fun(Node, Idx) ->transform('digit', Node, Idx) end).

-spec 'doubleq_string'(input(), index()) -> parse_result().
'doubleq_string'(Input, Index) ->
  p(Input, Index, 'doubleq_string', fun(I,D) -> (p_seq([p_string(<<"\"">>), p_label('chars', p_zero_or_more(p_seq([p_not(p_string(<<"\"">>)), p_choose([p_string(<<"\\\\">>), p_string(<<"\\\"">>), p_anything()])]))), p_string(<<"\"">>)]))(I,D) end, fun(Node, _Idx) ->iolist_to_binary(proplists:get_value(chars, Node)) end).

-spec 'singleq_string'(input(), index()) -> parse_result().
'singleq_string'(Input, Index) ->
  p(Input, Index, 'singleq_string', fun(I,D) -> (p_seq([p_string(<<"\'">>), p_label('chars', p_zero_or_more(p_seq([p_not(p_string(<<"\'">>)), p_choose([p_string(<<"\\\\">>), p_string(<<"\\\'">>), p_anything()])]))), p_string(<<"\'">>)]))(I,D) end, fun(Node, _Idx) ->iolist_to_binary(proplists:get_value(chars, Node)) end).

-spec 'spaces'(input(), index()) -> parse_result().
'spaces'(Input, Index) ->
  p(Input, Index, 'spaces', fun(I,D) -> (p_zero_or_more(p_charclass(<<"[\s\t\n\s\r]">>)))(I,D) end, fun(Node, Idx) ->transform('spaces', Node, Idx) end).


transform(_,Node,_Index) -> Node.
-file("peg_includes.hrl", 1).
-type index() :: {{line, pos_integer()}, {column, pos_integer()}}.
-type input() :: binary().
-type parse_failure() :: {fail, term()}.
-type parse_success() :: {term(), input(), index()}.
-type parse_result() :: parse_failure() | parse_success().
-type parse_fun() :: fun((input(), index()) -> parse_result()).
-type xform_fun() :: fun((input(), index()) -> term()).

-spec p(input(), index(), atom(), parse_fun(), xform_fun()) -> parse_result().
p(Inp, StartIndex, Name, ParseFun, TransformFun) ->
  case get_memo(StartIndex, Name) of      % See if the current reduction is memoized
    {ok, Memo} -> %Memo;                     % If it is, return the stored result
      Memo;
    _ ->                                        % If not, attempt to parse
      Result = case ParseFun(Inp, StartIndex) of
        {fail,_} = Failure ->                       % If it fails, memoize the failure
          Failure;
        {Match, InpRem, NewIndex} ->               % If it passes, transform and memoize the result.
          Transformed = TransformFun(Match, StartIndex),
          {Transformed, InpRem, NewIndex}
      end,
      memoize(StartIndex, Name, Result),
      Result
  end.

-spec setup_memo() -> ets:tid().
setup_memo() ->
  put({parse_memo_table, ?MODULE}, ets:new(?MODULE, [set])).

-spec release_memo() -> true.
release_memo() ->
  ets:delete(memo_table_name()).

-spec memoize(index(), atom(), parse_result()) -> true.
memoize(Index, Name, Result) ->
  Memo = case ets:lookup(memo_table_name(), Index) of
              [] -> [];
              [{Index, Plist}] -> Plist
         end,
  ets:insert(memo_table_name(), {Index, [{Name, Result}|Memo]}).

-spec get_memo(index(), atom()) -> {ok, term()} | {error, not_found}.
get_memo(Index, Name) ->
  case ets:lookup(memo_table_name(), Index) of
    [] -> {error, not_found};
    [{Index, Plist}] ->
      case proplists:lookup(Name, Plist) of
        {Name, Result}  -> {ok, Result};
        _  -> {error, not_found}
      end
    end.

-spec memo_table_name() -> ets:tid().
memo_table_name() ->
    get({parse_memo_table, ?MODULE}).

-ifdef(p_eof).
-spec p_eof() -> parse_fun().
p_eof() ->
  fun(<<>>, Index) -> {eof, [], Index};
     (_, Index) -> {fail, {expected, eof, Index}} end.
-endif.

-ifdef(p_optional).
-spec p_optional(parse_fun()) -> parse_fun().
p_optional(P) ->
  fun(Input, Index) ->
      case P(Input, Index) of
        {fail,_} -> {[], Input, Index};
        {_, _, _} = Success -> Success
      end
  end.
-endif.

-ifdef(p_not).
-spec p_not(parse_fun()) -> parse_fun().
p_not(P) ->
  fun(Input, Index)->
      case P(Input,Index) of
        {fail,_} ->
          {[], Input, Index};
        {Result, _, _} -> {fail, {expected, {no_match, Result},Index}}
      end
  end.
-endif.

-ifdef(p_assert).
-spec p_assert(parse_fun()) -> parse_fun().
p_assert(P) ->
  fun(Input,Index) ->
      case P(Input,Index) of
        {fail,_} = Failure-> Failure;
        _ -> {[], Input, Index}
      end
  end.
-endif.

-ifdef(p_seq).
-spec p_seq([parse_fun()]) -> parse_fun().
p_seq(P) ->
  fun(Input, Index) ->
      p_all(P, Input, Index, [])
  end.

-spec p_all([parse_fun()], input(), index(), [term()]) -> parse_result().
p_all([], Inp, Index, Accum ) -> {lists:reverse( Accum ), Inp, Index};
p_all([P|Parsers], Inp, Index, Accum) ->
  case P(Inp, Index) of
    {fail, _} = Failure -> Failure;
    {Result, InpRem, NewIndex} -> p_all(Parsers, InpRem, NewIndex, [Result|Accum])
  end.
-endif.

-ifdef(p_choose).
-spec p_choose([parse_fun()]) -> parse_fun().
p_choose(Parsers) ->
  fun(Input, Index) ->
      p_attempt(Parsers, Input, Index, none)
  end.

-spec p_attempt([parse_fun()], input(), index(), none | parse_failure()) -> parse_result().
p_attempt([], _Input, _Index, Failure) -> Failure;
p_attempt([P|Parsers], Input, Index, FirstFailure)->
  case P(Input, Index) of
    {fail, _} = Failure ->
      case FirstFailure of
        none -> p_attempt(Parsers, Input, Index, Failure);
        _ -> p_attempt(Parsers, Input, Index, FirstFailure)
      end;
    Result -> Result
  end.
-endif.

-ifdef(p_zero_or_more).
-spec p_zero_or_more(parse_fun()) -> parse_fun().
p_zero_or_more(P) ->
  fun(Input, Index) ->
      p_scan(P, Input, Index, [])
  end.
-endif.

-ifdef(p_one_or_more).
-spec p_one_or_more(parse_fun()) -> parse_fun().
p_one_or_more(P) ->
  fun(Input, Index)->
      Result = p_scan(P, Input, Index, []),
      case Result of
        {[_|_], _, _} ->
          Result;
        _ ->
          {fail, {expected, Failure, _}} = P(Input,Index),
          {fail, {expected, {at_least_one, Failure}, Index}}
      end
  end.
-endif.

-ifdef(p_label).
-spec p_label(atom(), parse_fun()) -> parse_fun().
p_label(Tag, P) ->
  fun(Input, Index) ->
      case P(Input, Index) of
        {fail,_} = Failure ->
           Failure;
        {Result, InpRem, NewIndex} ->
          {{Tag, Result}, InpRem, NewIndex}
      end
  end.
-endif.

-ifdef(p_scan).
-spec p_scan(parse_fun(), input(), index(), [term()]) -> {[term()], input(), index()}.
p_scan(_, <<>>, Index, Accum) -> {lists:reverse(Accum), <<>>, Index};
p_scan(P, Inp, Index, Accum) ->
  case P(Inp, Index) of
    {fail,_} -> {lists:reverse(Accum), Inp, Index};
    {Result, InpRem, NewIndex} -> p_scan(P, InpRem, NewIndex, [Result | Accum])
  end.
-endif.

-ifdef(p_string).
-spec p_string(binary()) -> parse_fun().
p_string(S) ->
    Length = erlang:byte_size(S),
    fun(Input, Index) ->
      try
          <<S:Length/binary, Rest/binary>> = Input,
          {S, Rest, p_advance_index(S, Index)}
      catch
          error:{badmatch,_} -> {fail, {expected, {string, S}, Index}}
      end
    end.
-endif.

-ifdef(p_anything).
-spec p_anything() -> parse_fun().
p_anything() ->
  fun(<<>>, Index) -> {fail, {expected, any_character, Index}};
     (Input, Index) when is_binary(Input) ->
          <<C/utf8, Rest/binary>> = Input,
          {<<C/utf8>>, Rest, p_advance_index(<<C/utf8>>, Index)}
  end.
-endif.

-ifdef(p_charclass).
-spec p_charclass(string() | binary()) -> parse_fun().
p_charclass(Class) ->
    {ok, RE} = re:compile(Class, [unicode, dotall]),
    fun(Inp, Index) ->
            case re:run(Inp, RE, [anchored]) of
                {match, [{0, Length}|_]} ->
                    {Head, Tail} = erlang:split_binary(Inp, Length),
                    {Head, Tail, p_advance_index(Head, Index)};
                _ -> {fail, {expected, {character_class, binary_to_list(Class)}, Index}}
            end
    end.
-endif.

-ifdef(p_regexp).
-spec p_regexp(binary()) -> parse_fun().
p_regexp(Regexp) ->
    {ok, RE} = re:compile(Regexp, [unicode, dotall, anchored]),
    fun(Inp, Index) ->
        case re:run(Inp, RE) of
            {match, [{0, Length}|_]} ->
                {Head, Tail} = erlang:split_binary(Inp, Length),
                {Head, Tail, p_advance_index(Head, Index)};
            _ -> {fail, {expected, {regexp, binary_to_list(Regexp)}, Index}}
        end
    end.
-endif.

-ifdef(line).
-spec line(index() | term()) -> pos_integer() | undefined.
line({{line,L},_}) -> L;
line(_) -> undefined.
-endif.

-ifdef(column).
-spec column(index() | term()) -> pos_integer() | undefined.
column({_,{column,C}}) -> C;
column(_) -> undefined.
-endif.

-spec p_advance_index(input() | unicode:charlist() | pos_integer(), index()) -> index().
p_advance_index(MatchedInput, Index) when is_list(MatchedInput) orelse is_binary(MatchedInput)-> % strings
  lists:foldl(fun p_advance_index/2, Index, unicode:characters_to_list(MatchedInput));
p_advance_index(MatchedInput, Index) when is_integer(MatchedInput) -> % single characters
  {{line, Line}, {column, Col}} = Index,
  case MatchedInput of
    $\n -> {{line, Line+1}, {column, 1}};
    _ -> {{line, Line}, {column, Col+1}}
  end.
