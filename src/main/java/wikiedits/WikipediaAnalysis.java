package wikiedits;

import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditEvent;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditsSource;

/**
 * wiki-edits
 * Created by yuqi
 * Date:17-1-21
 */
public class WikipediaAnalysis {

    public static void main (String [] args){
        StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        final DataStream<WikipediaEditEvent> editEventDataStream = streamExecutionEnvironment.addSource(new WikipediaEditsSource());

        KeyedStream<WikipediaEditEvent,String> keyedStream = editEventDataStream.keyBy(new KeySelector<WikipediaEditEvent, String>() {
            @Override
            public String getKey(WikipediaEditEvent wikipediaEditEvent) throws Exception {
                return wikipediaEditEvent.getUser();
            }
        });


        DataStream<Tuple2<String,Long>> result = keyedStream.timeWindow(Time.seconds(5)).fold(new Tuple2<>("", 0L), new FoldFunction<WikipediaEditEvent, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> fold(Tuple2<String, Long> stringLongTuple2, WikipediaEditEvent o) throws Exception {
                stringLongTuple2.f0 = o.getUser();
                stringLongTuple2.f1 += o.getByteDiff();

                return stringLongTuple2;
            }
        });

        System.out.println("--------------------------------------------");
        System.out.println(streamExecutionEnvironment.getExecutionPlan());
        System.out.println("--------------------------------------------");
        result.print();
        /*result.map(new MapFunction<Tuple2<String,Long>, String>() {
            @Override
            public String map(Tuple2<String,Long> tuple){
                return tuple.toString();
            }
        }).addSink(new )
        */

        try {
            streamExecutionEnvironment.execute();
            System.out.println("--------------------------------------------");
            System.out.println(streamExecutionEnvironment.getExecutionPlan());
            System.out.println("--------------------------------------------");
        }catch (Exception e){
            e.printStackTrace();
        }


    }
}
