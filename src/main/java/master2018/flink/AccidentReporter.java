package master2018.flink;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class AccidentReporter implements
		WindowFunction<Tuple8<Integer, Integer, Integer, Long, Integer, Boolean, Integer, Integer>, Tuple7<Integer, Integer, Integer, Integer, Integer, Boolean, Integer>, Tuple, TimeWindow> {

	@Override
	public void apply(Tuple key, TimeWindow window,
			Iterable<Tuple8<Integer, Integer, Integer, Long, Integer, Boolean, Integer, Integer>> input,
			Collector<Tuple7<Integer, Integer, Integer, Integer, Integer, Boolean, Integer>> out) throws Exception {

	}

}

