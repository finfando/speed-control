package master2018.flink;

import java.util.Iterator;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class AccidentReporter implements
		WindowFunction<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>, Tuple7<Integer, Integer, Integer, Integer, Integer, Integer, Integer>, Tuple, GlobalWindow> {

	@Override
	public void apply(Tuple key, GlobalWindow window,
			Iterable<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> input,
			Collector<Tuple7<Integer, Integer, Integer, Integer, Integer, Integer, Integer>> out) throws Exception {

		System.out.println("  ");
		System.out.println("TUPLE KEY " + key.toString());
		System.out.println("input " + input.toString());
		// collector se va rellenando e itera desde el principio de nuevo
		Iterator<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> iterator = input
				.iterator();

		Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> first = iterator.next();
		System.out.println(" ");
		System.out.println("First " + first.toString());
		int elements = 1;

		Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> tmp = null;

		while (iterator.hasNext()) {
			elements++;
			System.out.println("ELEMENTS " + elements);

			Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> currentElement = iterator
					.next();
			System.out.println("--> Current " + currentElement.toString());

			int previous_position;
			int current_position;
			if (elements == 2) {
				if (first.f7.equals(currentElement.f7)) {
					System.out.println("-----> E2 " + "     " + first.f7 + "   " + currentElement.f7 + " : "
							+ (first.f7.equals(currentElement.f7)));
					tmp = currentElement;
				}
				else {
					System.out.println("-E2  NULO");
					tmp = null;
				}

			}
			// 157437

			else if (elements == 3 && tmp != null) {
				System.out.println("-----> E3 " + "     " + tmp.f7 + "   " + currentElement.f7 + " : "
						+ (tmp.f7.equals(currentElement.f7)));
				if (tmp.f7.equals(currentElement.f7)) {

					tmp = currentElement;
				} else {
					System.out.println(" -->NO E3 ");
					tmp = null;
				}

			}

			else if (elements == 4 && tmp != null && tmp.f7.equals(currentElement.f7)) {
				System.out.println("****** Meeting Conditions " + tmp.toString());
				Integer time1 = first.f0;
				Integer time2 = currentElement.f0;
				Integer vid = currentElement.f1;
				Integer xWay = currentElement.f3;
				Integer seg = currentElement.f6;
				Integer dir = currentElement.f5;
				Integer pos = currentElement.f7;
				Tuple7<Integer, Integer, Integer, Integer, Integer, Integer, Integer> outTuple = new Tuple7(time1,
						time2, vid, xWay, seg, dir, pos);
				out.collect(outTuple);
			}

		}

	}
}

/*
 * 
 * while (iterator.hasNext()) { elements++; System.out.println("ELEMENTS " +
 * elements);
 * 
 * Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer,
 * Integer> currentElement = iterator .next(); System.out.println("--> Current "
 * + currentElement.toString());
 * 
 * int previous_position; int current_position; if (elements == 2 &&
 * first.f7.equals(currentElement.f7)) { System.out.println("-----> E2 " +
 * "     " + first.f7 + "   " + currentElement.f7 + " : " +
 * (first.f7.equals(currentElement.f7)));
 * 
 * tmp = currentElement;
 * 
 * } else { break; System.out.println("-E2  NULO"); tmp = null;
 * 
 * }
 * 
 * if (elements == 3 && tmp != null) { System.out.println(" -- E3 "); if
 * (tmp.f7.equals(currentElement.f7)) { tmp = currentElement; } else { tmp =
 * null; }
 * 
 * }
 * 
 * if (elements == 4 && tmp != null) {
 * System.out.println("****** Meeting Conditions " + tmp.toString()); Integer
 * time1 = first.f0; Integer time2 = tmp.f0; Integer vid = tmp.f1; Integer xWay
 * = tmp.f3; Integer seg = tmp.f6; Integer dir = tmp.f5; Integer pos = tmp.f7;
 * Tuple7<Integer, Integer, Integer, Integer, Integer, Integer, Integer>
 * outTuple = new Tuple7(time1, time2, vid, xWay, seg, dir, pos);
 * out.collect(outTuple); }
 * 
 * }
 * 
 */
