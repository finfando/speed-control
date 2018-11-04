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

		System.out.println("First " + first.toString());
		int elements = 1;

		while (iterator.hasNext()) {
			elements++;
			System.out.println("ELEMENTS " + elements);
			if (elements == 4) {
				Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> fourthEvent = iterator
						.next();
				Integer time1 = first.f0;
				Integer time2 = fourthEvent.f0;
				Integer vid = fourthEvent.f1;
				Integer xWay = fourthEvent.f3;
				Integer seg = fourthEvent.f6;
				Integer dir = fourthEvent.f5;
				Integer pos = fourthEvent.f7;
				Tuple7<Integer, Integer, Integer, Integer, Integer, Integer, Integer> outTuple = new Tuple7(time1,time2, vid, xWay, seg, dir, pos);
				out.collect(outTuple);
			} else {
				iterator.next();
			}

		}

	}
}
