package master2018.flink;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Iterator;

/**The purpose of the following class is to implement the WindowFunction interface in order to 
 * retrieve all those cars in  which  the average speed is higher than 60mph between the segments 52 and 56
 * Input :  Tuple6 <Tuple6<Time, VID, XWay, Dir, Seg, Pos>
 * Output : Tuple6 <Time1, Time2, VID, XWay, Dir, AvgSpd> in which avgspeed > 60mph between the segments 2 and 56
 */

public class AverageSpeedControl implements WindowFunction<
        Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>, Tuple6<Integer, Integer, Integer, Integer, Integer, Double>,
        Tuple, TimeWindow> {
   
	private static final long serialVersionUID = 1L;

	@Override
    public void apply(Tuple key, TimeWindow window, Iterable<Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>> input,
                      Collector<Tuple6<Integer, Integer, Integer, Integer, Integer, Double>> out) throws Exception {
        Iterator<Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>> iterator = input.iterator();
        Tuple6<Integer, Integer, Integer, Integer, Integer, Integer> first = iterator.next();
        Integer time1 = 0;
        Integer time2 = 0;
        Double timediff = 0.0;
        Integer vid = 0;
        Integer xway = 0;
        Integer dir1 = 0;
        Integer dir2 = null;
        Integer pos1 = 0;
        Integer pos2 = 0;
        Double avgspeed = 0.0;
        Integer seg1 = 0;
        Integer seg2 = 0;
        //first element in the window
        if(first != null){
            time1 = first.f0;
            vid = first.f1;
            pos1 = first.f5;
            seg1 = first.f4;
            xway = first.f2;
            dir1 = first.f3;
        }
        while(iterator.hasNext()){
            Tuple6<Integer, Integer, Integer, Integer, Integer, Integer> next = iterator.next();
            time2 = next.f0;
            pos2 = next.f5;
            seg2 = next.f4;
            dir2 = next.f3;
            //A car should go to the   with the same direction
            if(dir1 != dir2) {
                break;
            }
            timediff = 1.0*(time2-time1)/3600;
            avgspeed = Math.abs(pos1-pos2)/1609.344/timediff;
        }
        
        //filter by speed > 6'mph and segment between 52 and 56
        if ((seg1==52 || seg1==56) && (seg2==52 || seg2==56) && (seg1!=seg2) && (avgspeed > 60.0)) {

            out.collect(new Tuple6<>(time1, time2, vid, xway, dir1, avgspeed));
            
        }
    }
}