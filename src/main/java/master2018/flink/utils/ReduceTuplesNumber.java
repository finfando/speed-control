package master2018.flink.utils;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.tuple.Tuple8;

/**This class implements the MapFunction to reduce the number of tuples in the input Stream
 * to return only the elements required to perform the average Speed Operations
 * Input: Tuple8<Time, VID, Spd, XWay, Lane, Dir, Seg, Pos>
 * Output: Tuple6<Time, VID, XWay, Dir, Seg,Pos>
 */
public class ReduceTuplesNumber implements MapFunction<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>, Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>>{
	private static final long serialVersionUID = 1L;

	@Override
	public Tuple6<Integer, Integer, Integer, Integer, Integer, Integer> map(
			Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> in)
			throws Exception { 
		return new Tuple6<>(in.f0, in.f1, in.f3, in.f5, in.f6, in.f7); // T8 Time0, VID1,Spd2, XWay3,
																		// Lane4,Dir5, Seg6, Pos7
	}
}