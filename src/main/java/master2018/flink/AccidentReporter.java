package master2018.flink;

import java.util.Iterator;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**This class implements the WindowFunction to detect an accident if a cars is stopped for more than 4 consecutive events
 * Input:  Tuple6 <Tuple6<Time, VID, XWay, Dir, Seg,Pos>
 * Output: Tuple7<Time first event,Time 4th event, VID, XWay, Seg, Dir,Pos>
 */
public class AccidentReporter implements
		WindowFunction<Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>, Tuple7<Integer, Integer, Integer, Integer, Integer, Integer, Integer>, Tuple, GlobalWindow> {

	private static final long serialVersionUID = 1L;

	@Override
	public void apply(Tuple key, GlobalWindow window,
			Iterable<Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>> input,
			Collector<Tuple7<Integer, Integer, Integer, Integer, Integer, Integer, Integer>> out) throws Exception {
		 //T6  Time0, VID1,  XWay2, Dir3, Seg4, Pos5

		Iterator<Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>> iterator = input
				.iterator();

		Tuple6<Integer, Integer, Integer, Integer, Integer, Integer> first = iterator.next();
		Integer timeFirst= first.f0;
		Integer posFirst = first.f5;  
		int elements = 1;
		Tuple6<Integer, Integer, Integer, Integer, Integer, Integer> tmp = null;

		while (iterator.hasNext()) {
			elements++;
			Tuple6<Integer, Integer, Integer, Integer, Integer, Integer> currentElement = iterator
					.next();
			Integer posCurrent = currentElement.f5;
			if (elements == 2 && posFirst.equals(posCurrent)) {
					tmp = currentElement;
			}
			// 157437
			else if (elements == 3 && tmp != null) {
				if (tmp.f5.equals(posCurrent)) {
					tmp = currentElement;
				}
			}

			else if (elements == 4 && tmp != null) {
				if (tmp.f5.equals(currentElement.f5)) {
					//T6  Time0, VID1,  XWay2, Dir3, Seg4, Pos5
					Integer time2 = currentElement.f0;
					Integer vid = currentElement.f1;
					Integer xWay = currentElement.f2;
					Integer dir = currentElement.f3;
					Integer seg = currentElement.f4;
					
					out.collect(new Tuple7<Integer, Integer, Integer, Integer, Integer, Integer, Integer>(timeFirst,time2, vid, xWay, seg, dir, posCurrent));
				}
				
			}

		}

	}
}


