package master2018.flink;

import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class VehicleTelematics {
	public static void main(String[] args) {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		String inFilePath = "/home/yoss/Escritorio/inputData2.txt";
		String outFilePath = "/home/yoss/Escritorio/outputData2.txt";
		
		DataStreamSource<String> source = env.readTextFile(inFilePath);
		@SuppressWarnings("serial")
		SingleOutputStreamOperator<Tuple6<Integer,Integer,Long,Integer,Integer,Integer>> speedRadar = source.map(new MapImplementation()).filter(new SpeedRadar()).map(new FlatMapOutput());
		speedRadar.writeAsCsv(outFilePath, WriteMode.OVERWRITE);
		
		
				
		try {
			 env.execute();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
