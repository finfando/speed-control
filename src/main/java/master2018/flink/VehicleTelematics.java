package master2018.flink;

import java.util.Iterator;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.hadoop.shaded.com.google.common.collect.Iterators;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import master2018.flink.utils.FilterBySegment;

public class VehicleTelematics {
	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		// String inFilePath = args[0];
		// String outFilePath=args[1];

		String inFilePath = "/home/yoss/Escritorio/inputData2.txt";
		String outFilePath = "/home/yoss/Escritorio";

		DataStreamSource<String> source = env.readTextFile(inFilePath);
		SingleOutputStreamOperator<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> mapStream = source
				.map(new MapImplementation());

		// Time, VID, Spd, XWay, Lane, Dir, Seg, Pos

		SingleOutputStreamOperator<Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>> speedRadar = mapStream
				.filter(new SpeedRadar()).map(new FlatMapOutput());
		speedRadar.writeAsCsv(outFilePath + "/speedfines.csv", FileSystem.WriteMode.OVERWRITE);

		SingleOutputStreamOperator<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> filteredStream = mapStream
				.filter(new FilterBySegment());

		KeyedStream<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>, Tuple> keyedStream = filteredStream
				.assignTimestampsAndWatermarks(
						new AscendingTimestampExtractor<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>>() {
							@Override
							public long extractAscendingTimestamp(
									Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> element) {
								return element.f0 * 1000;
							}
						})
				.keyBy(1);

		SingleOutputStreamOperator<Tuple6<Integer, Integer, Integer, Integer, Integer, Double>> result = keyedStream
				.window(EventTimeSessionWindows.withGap(Time.minutes(1))).apply(new AverageSpeedControl()); // Time1,
																											// Time2,VID,XWay,Dir,AvgSpd
		result.writeAsCsv(outFilePath + "/avgspeedfines.csv", FileSystem.WriteMode.OVERWRITE);

		KeyedStream<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>, Tuple> keyedStreamByVID = mapStream
				.keyBy(1);
		SingleOutputStreamOperator<Tuple7<Integer, Integer, Integer, Integer, Integer, Integer, Integer>> accidentReporter = keyedStreamByVID
				.countWindow(4, 1).apply(new AccidentReporter());

		accidentReporter.writeAsCsv(outFilePath + "/accidents.csv", FileSystem.WriteMode.OVERWRITE);

		try {
			env.execute();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
}
