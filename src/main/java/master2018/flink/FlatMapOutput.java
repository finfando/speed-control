package master2018.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.util.Collector;

//FlatMapFunction that tokenizes a String by whitespace characters and emits all String tokens.
public class FlatMapOutput
		implements MapFunction<Tuple8<Integer, Integer, Integer, Long, Integer, Boolean, Integer, Integer>, Tuple6<Integer,Integer,Long,Integer,Integer,Integer>> {
	
	@Override
	public Tuple6<Integer,Integer,Long,Integer,Integer,Integer> map(Tuple8<Integer, Integer, Integer, Long, Integer, Boolean, Integer, Integer> in) throws Exception {
		// TODO Auto-generated method stub
		return new Tuple6<Integer,Integer,Long,Integer,Integer,Integer>(in.f0,in.f1,in.f3,in.f6,in.f7,in.f2);
	}



/*	@Override
	public void flatMap(Tuple8<Integer, Integer, Integer, Long, Integer, Boolean, Integer, Integer> in,
			Collector<T> out) throws Exception {
		//Time, VID, Spd, XWay, Lane, Dir, Seg, Pos
				if (type.equals("speed")) {
					//Time, VID, XWay, Seg, Dir, Spd
					//
					out.collect((T) new Tuple6<Integer,Integer,Long,Integer,Integer,Integer>(in.f0,in.f1,in.f3,in.f6,in.f7,in.f2));
				}
		
	}*/
	
	
/*
	@Override
	public T map(Tuple8<Integer, Integer, Integer, Long, Integer, Boolean, Integer, Integer> in) throws Exception {

		//Time, VID, Spd, XWay, Lane, Dir, Seg, Pos
		if (type.equals("speed")) {
			//Time, VID, XWay, Seg, Dir, Spd
			//
			return new Tuple6<Integer,Integer,Long,Integer,Integer,Integer>(in.f0,in.f1,in.f3,in.f6,in.f7,in.f2);
		}
		else{return null;}
	}
*/
}

/*
class AppendOne<T> implements FlatMapFunction<T, Tuple2<T, Long>> {

	public Tuple2<T, Long> map(T value) {
		return new Tuple2<T, Long>(value, 1L);
	}

	@Override
	public void flatMap(T value, Collector<<T, Long>> out) throws Exception {
		// TODO Auto-generated method stub
		
	}
}*/