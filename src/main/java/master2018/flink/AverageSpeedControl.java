package master2018.flink;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Iterator;

public class AverageSpeedControl implements WindowFunction<
        Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>, Tuple6<Integer, Integer, Integer, Integer, Integer, Double>,
        Tuple, TimeWindow> {
    /**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
    public void apply(Tuple key, TimeWindow window, Iterable<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> input,
                      Collector<Tuple6<Integer, Integer, Integer, Integer, Integer, Double>> out) throws Exception {
        Iterator<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> iterator = input.iterator();
        Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> first = iterator.next();
        Integer time1 = 0;
        Integer time2 = 0;
        Double timediff = 0.0;
        Integer vid = 0;
        Integer xway = 0;
        Integer dir = 0;
        Integer pos1 = 0;
        Integer pos2 = 0;
        Double avgspeed = 0.0;
        Integer seg1 = 0;
        Integer seg2 = 0;
        if(first != null){
            time1 = first.f0;
            vid = first.f1;
            pos1 = first.f7;
            seg1 = first.f6;
            xway = first.f3;
            dir = first.f5;
        }
        while(iterator.hasNext()){
            Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> next = iterator.next();
            time2 = next.f0;
            pos2 = next.f7;
            seg2 = next.f6;
            timediff = 1.0*(time2-time1)/3600;
            avgspeed = Math.abs(pos1-pos2)/1609.344/timediff;
        }
        if ((seg1==52 || seg1==56) && (seg2==52 || seg2==56) && (seg1!=seg2) && (avgspeed > 60.0)) {
//            Time1, Time2, VID, XWay, Dir, AvgSpd
            out.collect(new Tuple6<>(time1, time2, vid, xway, dir, avgspeed));
            
        }
    }
}