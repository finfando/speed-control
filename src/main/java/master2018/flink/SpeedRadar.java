package master2018.flink;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple8;


/**The purpose of the following class is to implement the flink filter function in order to 
 * filter all those events in  which a car has exceed the speed limit of 90mph 
 * Input : Tuple8<Time, VID, Spd, XWay, Lane, Dir, Seg, Pos>
 * Output : Tuple8<Time, VID, Spd, XWay, Lane, Dir, Seg, Pos> in which speed > 90mph
 */

public class SpeedRadar implements FilterFunction<Tuple8<Integer, Integer, Integer,Integer,Integer,Integer,Integer,Integer>>{
	
	private static final long serialVersionUID = 1L;

	@Override
	public boolean filter(Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> in)
			throws Exception {
//		System.out.println("Velocidad "+ in.f2);
		if (in.f2 > 90) {
			return true;
		} else {
			return false;
		}
	}

}
