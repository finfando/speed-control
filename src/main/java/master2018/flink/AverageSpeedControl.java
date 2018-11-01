package master2018.flink;

import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class AverageSpeedControl implements WindowFunction<Tuple8<Integer, Integer, Integer, Long, Integer, Boolean, Integer, Integer>, String, String, TimeWindow> {
    @Override
    public void apply(String key, TimeWindow window, Iterable<Tuple8<Integer, Integer, Integer, Long, Integer, Boolean, Integer, Integer>> input, Collector<String> out)
            throws Exception {
        long count = 0;
        for (Tuple8<Integer, Integer, Integer, Long, Integer, Boolean, Integer, Integer> in: input) {
            count++;
        }
        out.collect("Window: " + window + "count: " + count);
    }
}