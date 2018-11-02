package master2018.flink;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class VehicleTelematics {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        String inFilePath = args[0];
        DataStreamSource<String> source = env.readTextFile(inFilePath);
        SingleOutputStreamOperator<Tuple8<Integer, Integer, Integer, Long, Integer, Boolean, Integer, Integer>> mapStream = source.map(new MapImplementation());

        SingleOutputStreamOperator<Tuple6<Integer,Integer,Long,Integer,Integer,Integer>> speedRadar = mapStream.filter(new SpeedRadar()).map(new FlatMapOutput());
        speedRadar.writeAsCsv(args[1] + "/speedfines.csv", FileSystem.WriteMode.OVERWRITE);

        SingleOutputStreamOperator<Tuple8<Integer, Integer, Integer, Long, Integer, Boolean, Integer, Integer>> filteredStream = mapStream
                .filter(new FilterFunction<Tuple8<Integer, Integer, Integer, Long, Integer, Boolean, Integer, Integer>>() {
                    @Override
                    public boolean filter(Tuple8<Integer, Integer, Integer, Long, Integer, Boolean, Integer, Integer> in) throws Exception {
                        if (in.f6 >= 52 && in.f6 <= 56) {
                            return true;
                        } else {
                            return false;
                        }
                    }
                });

        KeyedStream<Tuple8<Integer, Integer, Integer, Long, Integer, Boolean, Integer, Integer>, Tuple> keyedStream = filteredStream
                .assignTimestampsAndWatermarks(
                        new AscendingTimestampExtractor<Tuple8<Integer, Integer, Integer, Long, Integer, Boolean, Integer, Integer>>(){
                            @Override
                            public long extractAscendingTimestamp(Tuple8<Integer, Integer, Integer, Long, Integer, Boolean, Integer, Integer> element) {
                                return element.f0*1000;
                            }
                        })
                .keyBy(1);

        SingleOutputStreamOperator<Tuple4<Integer, Integer, Integer, Integer>> result = keyedStream
                .window(EventTimeSessionWindows.withGap(Time.minutes(1)))
                .apply(new AverageSpeedControl());
//                .reduce(new ReduceFunction<Tuple8<Integer, Integer, Integer, Long, Integer, Boolean, Integer, Integer>>() {
//                    @Override
//                    public Tuple8<Integer, Integer, Integer, Long, Integer, Boolean, Integer, Integer> reduce(
//                            Tuple8<Integer, Integer, Integer, Long, Integer, Boolean, Integer, Integer> v1,
//                            Tuple8<Integer, Integer, Integer, Long, Integer, Boolean, Integer, Integer> v2) throws Exception {
//                        Tuple8<Integer, Integer, Integer, Long, Integer, Boolean, Integer, Integer> out = new Tuple8<>(v1.f0, v1.f1, v1.f2, v1.f3, v1.f4, v1.f5, v1.f6, v1.f7);
//                        return out;
//                    }
//                });
//        Time1, Time2, VID, XWay, Dir, AvgSpd
        result.writeAsCsv(args[1] + "/avgspeedfines.csv", FileSystem.WriteMode.OVERWRITE);


//        SingleOutputStreamOperator<Tuple8<Integer, Integer, Integer, Long, Integer, Boolean, Integer, Integer>> accidentReporter = mapStream;
//        accidentReporter.writeAsCsv(args[1] + "/accidents.csv", FileSystem.WriteMode.OVERWRITE);

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}

