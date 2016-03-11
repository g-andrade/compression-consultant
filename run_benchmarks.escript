%!/usr/bin/env script
% vim: set ft=erlang expandtab softtabstop=4 shiftwidth=4:
-mode(compile).

-define(DEFAULT_ZLIB_LEVEL, default).
-define(DEFAULT_ZLIB_WINDOW_BITS, 15).
-define(DEFAULT_ZLIB_MEM_LEVEL, 8).
-define(DEFAULT_ZLIB_STRATEGY, default).

-define(DEFAULT_COMPRESSION_THRESHOLD, 0).

main(Args) ->
    {PacketLogPaths, Runs} = parse_args(Args),
    run_benchmarks(PacketLogPaths, Runs).

parse_args(Args) ->
    [StrRuns | [_|_] = PacketLogPaths] = Args,
    Runs = max(1, list_to_integer(StrRuns)),
    {PacketLogPaths, Runs}.

run_benchmarks(PacketLogPaths, Runs) ->
    Benchmarks = [
                  %{zlib, stateful, [{threshold, infinity}]},
                  %{zlib, stateless, [{level, 9}, {memlevel, 9}]},
                  {zlib, stateless, [{threshold, 30}]},
                  {zlib, stateful, [{threshold, 30}]}
                  %{zlib, stateful, [{strategy, huffman_only}]},
                  %{zlib, stateful, [{strategy, filtered}]}
                 ],
    lists:foreach(
      fun (Benchmark) ->
              run_benchmark(PacketLogPaths, Runs, Benchmark)
      end,
      Benchmarks).

run_benchmark(PacketLogPaths, Runs, Benchmark) ->
    {FinalRatios, AllStats} =
        lists:foldl(
          fun (PacketLogPath, {RatiosAcc, StatsAcc}) ->
                  {Ratios, Stats} = run_benchmark_for_log(PacketLogPath, Runs, Benchmark),
                  {Ratios ++ RatiosAcc,
                   Stats ++ StatsAcc}
          end,
          {[], []}, PacketLogPaths),
    Results = generate_benchmark_results(FinalRatios, AllStats, Runs),
    print_benchmark_results(Benchmark, Results).

print_benchmark_results(Benchmark, Results) ->
    io:format("~p:~n\t~p~n****************~n",
              [Benchmark, Results]).

generate_benchmark_results(FinalRatios, AllStats, Runs) ->
    {SumRatios, MinRatio, MaxRatio} =
        lists:foldl(
          fun (Ratio, {SumRatiosAcc, PrevMinRatio, PrevMaxRatio}) ->
                  {SumRatiosAcc + Ratio,
                   min(PrevMinRatio, Ratio),
                   max(PrevMaxRatio, Ratio)}
          end,
          erlang:make_tuple(3, hd(FinalRatios)),
          tl(FinalRatios)),
    AvgRatio = SumRatios / length(FinalRatios),
    NormalizedStats = normalize_stats(AllStats, Runs),
    [{avg_ratio, format_ratio(AvgRatio)},
     {worst_ratio, format_ratio(MaxRatio)},
     {best_ratio, format_ratio(MinRatio)}]
    ++ NormalizedStats.

format_ratio(Ratio) ->
    %io_lib:format("~B%", [trunc(Ratio * 100)]).
    trunc(Ratio * 100).

normalize_stats(AllStats, Runs) ->
    Bucketed = lists:foldl(
                 fun (Stats, AccA) ->
                         lists:foldl(
                           fun ({K, V}, AccB) ->
                                   dict:append(K, V, AccB)
                           end,
                           AccA,
                           Stats)
                 end,
                 dict:new(),
                 AllStats),
    Normalized = dict:map(fun (K, V) -> trunc(lists:sum(V) / Runs) end,
                        Bucketed),
    dict:to_list(Normalized).

run_benchmark_for_log(PacketLogPath, Runs, Benchmark) ->
    {ok, Log} = file:open(PacketLogPath, [read]),
    Packets = (fun ReadLine(Acc) ->
                       case file:read_line(Log) of
                           {ok, Line} ->
                               DecodedLine = base64:decode(Line),
                               ReadLine([DecodedLine | Acc]);
                           eof ->
                               file:close(Log),
                               lists:reverse(Acc)
                       end
               end)([]),

    BenchmarkFoldFun1 = fun (Packet, {RatiosAcc, StatsAcc, PrevState}) ->
                               {Ratios, Stats, NewState} = benchmark_step(Benchmark,
                                                                          Packet, PrevState),
                               {[Ratios | RatiosAcc],
                                [Stats | StatsAcc],
                                NewState}
                       end,
    BenchmarkFoldFunN = fun (Packet, {StatsAcc, PrevState}) ->
                                {_Ratios, Stats, NewState} = benchmark_step(Benchmark,
                                                                            Packet, PrevState),
                                {[Stats | StatsAcc],
                                 NewState}
                        end,

    {Ratios1, AllStats} = (fun Run(0=_RunsSoFar, []=_RatiosAcc, []=_StatsAcc) ->
                                   State = benchmark_init(Benchmark),
                                   {Ratios, Stats, FinalState} = lists:foldl(BenchmarkFoldFun1,
                                                                             {[], [], State},
                                                                             Packets),
                                   benchmark_terminate(Benchmark, FinalState),
                                   Run(1, Ratios, Stats);
                               Run(RunsSoFar, RatiosAcc, StatsAcc) when RunsSoFar >= Runs ->
                                   {RatiosAcc, StatsAcc};
                               Run(RunsSoFar, RatiosAcc, StatsAcc) ->
                                   State = benchmark_init(Benchmark),
                                   {Stats, FinalState} = lists:foldl(BenchmarkFoldFunN,
                                                                     {[], State},
                                                                     Packets),
                                   benchmark_terminate(Benchmark, FinalState),
                                   Run(RunsSoFar + 1, RatiosAcc, Stats ++ StatsAcc)
                           end)(0, [], []),
    {Ratios1, AllStats}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
benchmark_init({zlib, stateless, _Params}) ->
    no_state;
benchmark_init({zlib, stateful, Params}) ->
    ZStream = zlib:open(),
    Level = proplists:get_value(level, Params, ?DEFAULT_ZLIB_LEVEL),
    Method = deflated,
    WindowBits = proplists:get_value(window_bits, Params, ?DEFAULT_ZLIB_WINDOW_BITS),
    MemLevel = proplists:get_value(mem_level, Params, ?DEFAULT_ZLIB_MEM_LEVEL),
    Strategy = proplists:get_value(strategy, Params, ?DEFAULT_ZLIB_STRATEGY),
    zlib:deflateInit(ZStream, Level, Method,
                     WindowBits, MemLevel, Strategy),
    ZStream.

benchmark_step({zlib, stateless, Params}, Packet, no_state) ->
    {Stats, Compressed} =
        with_stats(
          optional_compression_fun(
            stateless_compression_fun(Packet, Params),
            Packet, Params)),
    Ratio = iolist_size(Compressed) / iolist_size(Packet),
    {Ratio, Stats, no_state};
benchmark_step({zlib, stateful, Params}, Packet, ZStream) ->
    {Stats, Compressed} =
        with_stats(
          optional_compression_fun(fun () -> zlib:deflate(ZStream, Packet, sync) end,
                                   Packet, Params)),
    Ratio = iolist_size(Compressed) / iolist_size(Packet),
    {Ratio, Stats, ZStream}.

benchmark_terminate({zlib, stateless, _Params}, no_state) ->
    ok;
benchmark_terminate({zlib, stateful, _Params}, ZStream) ->
    catch zlib:deflateEnd(ZStream),
    zlib:close(ZStream).

optional_compression_fun(Fun, Packet, Params) ->
    Threshold = proplists:get_value(threshold, Params, ?DEFAULT_COMPRESSION_THRESHOLD),
    case byte_size(Packet) >= Threshold  of
        true  -> fun () ->
                         Compressed = Fun(),
                         case iolist_size(Compressed) < iolist_size(Packet) of
                             true  -> Compressed;
                             false -> Packet
                         end
                 end;
        false -> fun () -> Packet end
    end.

stateless_compression_fun(Packet, Params) ->
    Level = proplists:get_value(level, Params, ?DEFAULT_ZLIB_LEVEL),
    Method = deflated,
    WindowBits = proplists:get_value(window_bits, Params, ?DEFAULT_ZLIB_WINDOW_BITS),
    MemLevel = proplists:get_value(mem_level, Params, ?DEFAULT_ZLIB_MEM_LEVEL),
    Strategy = proplists:get_value(strategy, Params, ?DEFAULT_ZLIB_STRATEGY),
    fun () ->
            ZStream = zlib:open(),
            zlib:deflateInit(ZStream, Level, Method,
                             WindowBits, MemLevel, Strategy),
            Result = zlib:deflate(ZStream, Packet, finish),
            catch zlib:deflateEnd(ZStream),
            zlib:close(ZStream),
            Result
    end.

with_stats(Fun) ->
    {Time, {Reductions, Result}} =
        timer:tc(
          fun () ->
                  {reductions, ReductionsBefore} = erlang:process_info(self(), reductions),
                  Result = Fun(),
                  {reductions, ReductionsAfter} = erlang:process_info(self(), reductions),
                  {ReductionsAfter - ReductionsBefore, Result}
          end),
    {[{time, Time},
      {reductions, Reductions}],
     Result}.
