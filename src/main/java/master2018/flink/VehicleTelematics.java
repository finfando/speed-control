package master2018.flink;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class VehicleTelematics {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
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
//        KeyedStream<Tuple8<Integer, Integer, Integer, Long, Integer, Boolean, Integer, Integer>, Tuple> keyedStream = filteredStream.keyBy(1);
//        SingleOutputStreamOperator<Tuple8<Integer, Integer, Integer, Long, Integer, Boolean, Integer, Integer>> result = keyedStream
//                .window(ProcessingTimeSessionWindows.withGap(Time.milliseconds(10)))
//                .apply(new AverageSpeedControl());
        filteredStream.writeAsCsv(args[1] + "/avgspeedfines.csv", FileSystem.WriteMode.OVERWRITE);


//        SingleOutputStreamOperator<Tuple8<Integer, Integer, Integer, Long, Integer, Boolean, Integer, Integer>> accidentReporter = mapStream;
//        accidentReporter.writeAsCsv(args[1] + "/accidents.csv", FileSystem.WriteMode.OVERWRITE);

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}

