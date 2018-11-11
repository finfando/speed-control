package master2018.flink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import master2018.flink.*;
import master2018.flink.utils.FilterBySegment;
import master2018.flink.utils.FilterBySpeed;

public class VehicleTelematics {
	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		String inFilePath = args[0];
		String outFilePath = args[1];

		DataStreamSource<String> source = env.readTextFile(inFilePath).setParallelism(1);
		SingleOutputStreamOperator<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> mapStream = source
				.map(new MapImplementation()).setParallelism(1);
		// Time, VID, Spd, XWay, Lane, Dir, Seg, Pos

		SingleOutputStreamOperator<Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>> speedRadar = mapStream
				.filter(new SpeedRadar()).map(new FlatMapOutput());
		speedRadar.writeAsCsv(outFilePath + "/speedfines.csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

		SingleOutputStreamOperator<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> filteredStream = mapStream
                .filter(new FilterBySegment()).setParallelism(1);

		KeyedStream<Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>, Tuple> keyedStream = filteredStream
				.assignTimestampsAndWatermarks(
						new AscendingTimestampExtractor<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>>() {
							private static final long serialVersionUID = 1L;

							@Override
							public long extractAscendingTimestamp(
									Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> element) {
								return element.f0 * 1000;
							}
						}).setParallelism(1)
                .map(new MapFunction<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>, Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>>() {
                    private static final long serialVersionUID = 1L;
                    @Override
                    public Tuple6<Integer, Integer, Integer, Integer, Integer, Integer> map(
                            Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> in)
                            throws Exception {
                        return new Tuple6<>(in.f0, in.f1, in.f3, in.f5, in.f6, in.f7);
                    }
                }).setParallelism(1)
                .keyBy(1, 3);

		SingleOutputStreamOperator<Tuple6<Integer, Integer, Integer, Integer, Integer, Double>> avgSpeedControl = keyedStream
				.window(EventTimeSessionWindows.withGap(Time.seconds(180))).apply(new AverageSpeedControl());
		avgSpeedControl.writeAsCsv(outFilePath + "/avgspeedfines.csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

		KeyedStream<Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>, Tuple> keyedStreamByVID = mapStream
				.filter(new FilterBySpeed())
				.map(new MapFunction<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>, Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Tuple6<Integer, Integer, Integer, Integer, Integer, Integer> map(
							Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> in)
							throws Exception { // time vid xway seg dir pos

						return new Tuple6<>(in.f0, in.f1, in.f3, in.f5, in.f6, in.f7); // T8 Time0, VID1,Spd2, XWay3,
																						// Lane4,Dir5, Seg6, Pos7
					}
				}).keyBy(1);

		SingleOutputStreamOperator<Tuple7<Integer, Integer, Integer, Integer, Integer, Integer, Integer>> accidentReporter = keyedStreamByVID
				.countWindow(4, 1).apply(new AccidentReporter());
		accidentReporter.writeAsCsv(outFilePath + "/accidents.csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

		try {
			env.execute();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
}
