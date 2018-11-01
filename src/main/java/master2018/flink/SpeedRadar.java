package master2018.flink;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple8;

//Time, VID, Spd, XWay, Lane, Dir, Seg, Pos
public class SpeedRadar implements FilterFunction<Tuple8<Integer, Integer, Integer,Long,Integer,Boolean,Integer,Integer>>{
	@Override
	public boolean filter(Tuple8<Integer, Integer, Integer, Long, Integer, Boolean, Integer, Integer> in)
			throws Exception {
//		System.out.println("Velocidad "+ in.f2);
		if (in.f2 > 80) {
			return true;
		} else {
			return false;
		}
	}

}
