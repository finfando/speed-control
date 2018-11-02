package master2018.flink;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Iterator;

public class AverageSpeedControl implements WindowFunction<
        Tuple8<Integer, Integer, Integer, Long, Integer, Boolean, Integer, Integer>, Tuple4<Integer, Integer, Integer, Integer>,
        Tuple, TimeWindow> {
    @Override
    public void apply(Tuple key, TimeWindow window, Iterable<Tuple8<Integer, Integer, Integer, Long, Integer, Boolean, Integer, Integer>> input,
                      Collector<Tuple4<Integer, Integer, Integer, Integer>> out) throws Exception {
        Iterator<Tuple8<Integer, Integer, Integer, Long, Integer, Boolean, Integer, Integer>> iterator = input.iterator();
        Tuple8<Integer, Integer, Integer, Long, Integer, Boolean, Integer, Integer> first = iterator.next();
        Integer time1 = 0;
        Integer time2 = 0;
        Integer vid = 0;
        Integer pos1 = 0;
        Integer pos2 = 0;
        Integer distance = 0;
        Integer seg1 = 0;
        Integer seg2 = 0;
        if(first != null){
            time1 = first.f0;
            vid = first.f1;
            pos1 = first.f7;
            seg1 = first.f6;
        }
        while(iterator.hasNext()){
            Tuple8<Integer, Integer, Integer, Long, Integer, Boolean, Integer, Integer> next = iterator.next();
            time2 = next.f0;
            pos2 = next.f7;
            distance = Math.abs(pos1-pos2);
            seg2 = next.f6;
        }
        if ((seg1==52 || seg1==56) && (seg2==52 || seg2==56) && (seg1!=seg2)) {
            out.collect(new Tuple4<>(time1, time2, vid, distance));
        }
//        long count = 0;
//        for (Tuple8<Integer, Integer, Integer, Long, Integer, Boolean, Integer, Integer> in: input) {
//            count++;
//        }
//        out.collect("Window: " + window + "count: " + count);
    }
}