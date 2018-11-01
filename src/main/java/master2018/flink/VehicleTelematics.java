package master2018.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class VehicleTelematics {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        String inFilePath = args[0];
        DataStreamSource<String> source = env.readTextFile(inFilePath);
        SingleOutputStreamOperator<Tuple8<Integer, Integer, Integer, Long, Integer, Boolean, Integer, Integer>> mapStream = source.map(new MapImplementation());

        SingleOutputStreamOperator<Tuple6<Integer,Integer,Long,Integer,Integer,Integer>> speedRadar = mapStream.filter(new SpeedRadar()).map(new FlatMapOutput());
        speedRadar.writeAsCsv(args[1] + "/speedfines.csv", FileSystem.WriteMode.OVERWRITE);

//        SingleOutputStreamOperator<Tuple8<Integer, Integer, Integer, Long, Integer, Boolean, Integer, Integer>> averageSpeedControl = mapStream;
//        averageSpeedControl.writeAsCsv(args[1] + "/avgspeedfines.csv", FileSystem.WriteMode.OVERWRITE);
//
//        SingleOutputStreamOperator<Tuple8<Integer, Integer, Integer, Long, Integer, Boolean, Integer, Integer>> accidentReporter = mapStream;
//        accidentReporter.writeAsCsv(args[1] + "/accidents.csv", FileSystem.WriteMode.OVERWRITE);





//        flatMapOut.writeAsCsv(outFilePath, FileSystem.WriteMode.OVERWRITE);

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}