package master2018.flink;


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

import master2018.flink.utils.FilterBySegment;
import master2018.flink.utils.FilterBySpeed;
import master2018.flink.utils.ReduceTuplesNumber;

public class VehicleTelematics {
	public static void main(String[] args) throws Exception {
		//Creates a local execution environment that represents the context in which the program is currently executed. 
		
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		// Events’ timestamps, meaning each element in the stream needs to have its event timestamp assigned
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		String inFilePath = args[0];
		String outFilePath = args[1];
		
		//each event is a line as String
		DataStreamSource<String> source = env.readTextFile(inFilePath).setParallelism(1);//
		
		//Split every line(event) coming from the source file into Tuples8<Time,VID,Spd,Xway,Lane,Dir,Seg,Pos>
		SingleOutputStreamOperator<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> mapStream = source
				.map(new MapImplementation()).setParallelism(1);//
		
		/******************Exercise 1: Speed Radar****************/
		//filter the events in which speed > 90 and map the values required into a Tuple 6 with format Time,VID,Sway,Seg,Dir,Spd
		SingleOutputStreamOperator<Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>> speedRadar = mapStream
				.filter(new SpeedRadar()).map(new FlatMapOutput());
		
		//Writing speedRadar results into speedfines.csv. Parallelism of 1 is used to create a single file only.Otherwise, it creates 10 files
		speedRadar.writeAsCsv(outFilePath + "/speedfines.csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
			
		SingleOutputStreamOperator<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> filteredStream = mapStream
                .filter(new FilterBySegment()).setParallelism(1);

		
		/******************Exercise 2: Average Speed Control****************/
		/*- Detects Cars with an average speed higher than 60mph between segments 52 and 56 (inclusively).
		 *  In case that a car sends several reports on those segments,the average speed will be the events that cover longer distances-*/
		
		//Assigning timestamp and watermarks to the events. Useful elements are retrieved by the ReduceTuplesNumber class.
		//Events are are grouped by Car and Direction?
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
                .map(new ReduceTuplesNumber()).setParallelism(1)
                .keyBy(1, 3);
		
		//Creates a Event Time Window with a time gap of 180 in which if a new event is not received  in 180 there will be a session timeout 
		//and a new window session will be open for the upcoming events. Once that the window is closed the apply function is triggered to
		//obtain the average speed of every car in the segments 52 to 56
		SingleOutputStreamOperator<Tuple6<Integer, Integer, Integer, Integer, Integer, Double>> avgSpeedControl = keyedStream
				.window(EventTimeSessionWindows.withGap(Time.seconds(180))).apply(new AverageSpeedControl());
		
		//Writing Average Speed Control results into speedfines.csv. Parallelism of 1 is used to create a single file only.Otherwise, it creates 10 files
		avgSpeedControl.writeAsCsv(outFilePath + "/avgspeedfines.csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
		
		

		/******************Exercise 3: Accident Reporter****************/
		
		//Needed elements to evaluate a car accident are filtered by the ReduceTuplesNumber class. Events are are grouped by Car and Direction? 
		KeyedStream<Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>, Tuple> keyedStreamByVID = mapStream
				.filter(new FilterBySpeed())
				.map(new ReduceTuplesNumber()).keyBy(1, 3); 
		
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
