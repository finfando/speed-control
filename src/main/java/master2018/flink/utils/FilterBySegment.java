package master2018.flink.utils;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple8;

public class FilterBySegment
		implements FilterFunction<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> {
	private static final long serialVersionUID = 1L;
	@Override
	public boolean filter(Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> in)
			throws Exception {
		return in.f6 == 52 || in.f6 == 56;
	}
}