package master2018.flink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple8;

public class MapImplementation implements MapFunction<String, Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>>{
	private static final long serialVersionUID = 1L;

	@Override
	public Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> map(String in)
			throws Exception {
		String[] fieldArray = in.split(",");
		Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> out = new Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>(
				Integer.parseInt(fieldArray[0]), Integer.parseInt(fieldArray[1]),
				Integer.parseInt(fieldArray[2]), Integer.parseInt(fieldArray[3]),
				Integer.parseInt(fieldArray[4]), Integer.parseInt(fieldArray[5]),
				Integer.parseInt(fieldArray[6]), Integer.parseInt(fieldArray[7]));
		return out;
	}
}
	
//Time, VID, Spd, XWay, Lane, Dir, Seg, Pos
	

