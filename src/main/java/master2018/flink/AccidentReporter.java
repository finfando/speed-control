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

public class AccidentReporter implements
		WindowFunction<Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>, Tuple7<Integer, Integer, Integer, Integer, Integer, Integer, Integer>, Tuple, GlobalWindow> {

	private static final long serialVersionUID = 1L;

	@Override
	public void apply(Tuple key, GlobalWindow window,
			Iterable<Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>> input,
			Collector<Tuple7<Integer, Integer, Integer, Integer, Integer, Integer, Integer>> out) throws Exception {
		 //T6  Time0, VID1,  XWay2, Dir3, Seg4, Pos5

		//		System.out.println("  ");
//		System.out.println("TUPLE KEY " + key.toString());
//		System.out.println("input " + input.toString());
		
		Iterator<Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>> iterator = input
				.iterator();

		Tuple6<Integer, Integer, Integer, Integer, Integer, Integer> first = iterator.next();
		Integer timeFirst= first.f0;
		Integer posFirst = first.f5;  
//		System.out.println(" ");
//		System.out.println("First " + first.toString());
		int elements = 1;

		Tuple6<Integer, Integer, Integer, Integer, Integer, Integer> tmp = null;

		while (iterator.hasNext()) {
			elements++;
//			System.out.println("ELEMENTS " + elements);
			
			Tuple6<Integer, Integer, Integer, Integer, Integer, Integer> currentElement = iterator
					.next();
//			System.out.println("--> Current " + currentElement.toString());
			Integer posCurrent = currentElement.f5;
			if (elements == 2) {
				if (posFirst.equals(posCurrent)) {
//					System.out.println("-----> E2 " + "     " + first.f7 + "   " + currentElement.f7 + " : "
//							+ (first.f7.equals(currentElement.f7)));
					tmp = currentElement;
				}
			}
			// 157437

			else if (elements == 3 && tmp != null) {
//				System.out.println("-----> E3 " + "     " + tmp.f7 + "   " + currentElement.f7 + " : "
//						+ (tmp.f7.equals(currentElement.f7)));
				if (tmp.f5.equals(posCurrent)) {
					tmp = currentElement;
				}
			}

			else if (elements == 4 && tmp != null) {
//				System.out.println("$ENTRO CUATRO FOR " +currentElement.f1+"  tmp.f7 "+tmp.f7 +"  currentElement.f7  "+ currentElement.f7);
				if (tmp.f5.equals(currentElement.f5)) {
//					System.out.println("****** Meeting Conditions " + tmp.toString());
					//T6  Time0, VID1,  XWay2, Dir3, Seg4, Pos5
					Integer time2 = currentElement.f0;
					Integer vid = currentElement.f1;
					Integer xWay = currentElement.f2;
					Integer dir = currentElement.f3;
					Integer seg = currentElement.f4;
					Tuple7<Integer, Integer, Integer, Integer, Integer, Integer, Integer> outTuple = new Tuple7(timeFirst,
							time2, vid, xWay, seg, dir, posCurrent);
					out.collect(outTuple);
				}
				
			}

		}

	}
}


