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

public class VehicleTelematics {
	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		// String inFilePath = args[0];
		// String outFilePath=args[1];

		String inFilePath = "/home/yoss/Escritorio/inputData2.txt";
		String outFilePath = "/home/yoss/Escritorio";

		DataStreamSource<String> source = env.readTextFile(inFilePath);
		SingleOutputStreamOperator<Tuple8<Integer, Integer, Integer, Long, Integer, Boolean, Integer, Integer>> mapStream = source
				.map(new MapImplementation());

		/*
		 * SingleOutputStreamOperator<Tuple6<Integer,Integer,Long,Integer,Integer,
		 * Integer>> speedRadar = mapStream.filter(new SpeedRadar()).map(new
		 * FlatMapOutput()); speedRadar.writeAsCsv(outFilePath+ "/speedfines.csv",
		 * FileSystem.WriteMode.OVERWRITE);
		 * 
		 * SingleOutputStreamOperator<Tuple8<Integer, Integer, Integer, Long, Integer,
		 * Boolean, Integer, Integer>> filteredStream = mapStream .filter(new
		 * FilterFunction<Tuple8<Integer, Integer, Integer, Long, Integer, Boolean,
		 * Integer, Integer>>() {
		 * 
		 * @Override public boolean filter(Tuple8<Integer, Integer, Integer, Long,
		 * Integer, Boolean, Integer, Integer> in) throws Exception { if (in.f6 >= 52 &&
		 * in.f6 <= 56) { return true; } else { return false; } } });
		 * 
		 * KeyedStream<Tuple8<Integer, Integer, Integer, Long, Integer, Boolean,
		 * Integer, Integer>, Tuple> keyedStream = filteredStream
		 * .assignTimestampsAndWatermarks( new
		 * AscendingTimestampExtractor<Tuple8<Integer, Integer, Integer, Long, Integer,
		 * Boolean, Integer, Integer>>(){
		 * 
		 * @Override public long extractAscendingTimestamp(Tuple8<Integer, Integer,
		 * Integer, Long, Integer, Boolean, Integer, Integer> element) { return
		 * element.f0*1000; } }) .keyBy(1);
		 * 
		 * SingleOutputStreamOperator<Tuple6<Integer, Integer, Integer, Long, Boolean,
		 * Double>> result = keyedStream
		 * .window(EventTimeSessionWindows.withGap(Time.minutes(1))) .apply(new
		 * AverageSpeedControl()); // Time1, Time2, VID, XWay, Dir, AvgSpd
		 * result.writeAsCsv(outFilePath + "/avgspeedfines.csv",
		 * FileSystem.WriteMode.OVERWRITE);
		 * 
		 */
		KeyedStream<Tuple8<Integer, Integer, Integer, Long, Integer, Boolean, Integer, Integer>, Tuple> keyedStreamByVID = mapStream
				.keyBy(1);
		SingleOutputStreamOperator<Tuple7<Integer, Integer, Integer, Integer, Integer, Integer, Boolean>> accidentReporter = keyedStreamByVID
				.countWindow(4, 1)
				.apply(new WindowFunction<Tuple8<Integer, Integer, Integer, Long, Integer, Boolean, Integer, Integer>, Tuple7<Integer, Integer, Integer, Integer, Integer, Integer, Boolean>, Tuple, GlobalWindow>() {

					@Override
					public void apply(Tuple key, GlobalWindow window,
							Iterable<Tuple8<Integer, Integer, Integer, Long, Integer, Boolean, Integer, Integer>> input,
							Collector<Tuple7<Integer, Integer, Integer, Integer, Integer, Integer, Boolean>> out)
							throws Exception {

						System.out.println("  ");
						System.out.println("TUPLE KEY " + key.toString());
						System.out.println("input " + input.toString());
						// collector se va rellenando e itera desde el principio de nuevo
						Iterator<Tuple8<Integer, Integer, Integer, Long, Integer, Boolean, Integer, Integer>> iterator = input
								.iterator();

						Tuple8<Integer, Integer, Integer, Long, Integer, Boolean, Integer, Integer> first = iterator
								.next();

						int size = Iterators.size(iterator);
						System.out.println("size " + size);
						System.out.println("First " + first.toString());
						int elements = 1;

						while (iterator.hasNext()) {
							elements++;

							if (elements == 4) {
								System.out.println("Prueba COUNT " + elements);
							}

							// tiene un segundo, tercero
						}

						if (elements == 4) {

						}

					}
				});

		accidentReporter.writeAsCsv(outFilePath + "/accidents.csv", FileSystem.WriteMode.OVERWRITE);

		try {
			env.execute();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
}
