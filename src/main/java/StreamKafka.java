
import facebook.avro.FacebookPost;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer08;
import org.apache.flink.util.Collector;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import edu.stanford.nlp.coref.CorefCoreAnnotations;

import edu.stanford.nlp.coref.data.CorefChain;
import edu.stanford.nlp.io.*;
import edu.stanford.nlp.ling.*;
import edu.stanford.nlp.pipeline.*;
import edu.stanford.nlp.semgraph.SemanticGraph;
import edu.stanford.nlp.semgraph.SemanticGraphCoreAnnotations;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.trees.*;
import edu.stanford.nlp.util.*;

/**
 * Created by isaac on 4/2/17.
 */
public class StreamKafka {
    // Function to return Kafka properties
    private static Properties getProperties(){
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("zookeeper.connect", "localhost:2181");
        properties.setProperty("group.id", "fb");
        return properties;

    }
    public static void main(String[] args){
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = getProperties();
        //Initialize the deserialization schema
        AvroDeserializationSchema<FacebookPost> aPost = new AvroDeserializationSchema<>(FacebookPost.class);

        DataStream<FacebookPost> stream = env
                .addSource(new FlinkKafkaConsumer08<>("fb", aPost, properties));

        DataStream<String> s = stream.map(new StatusMessage());
        s.map(new NLP()).print();

        DataStream<Tuple2<String,Integer>> finalCount = s.flatMap(new TokenizeTerms()).filter(new RelevantTerms("somefile.txt"));
        finalCount.keyBy(0).sum(1).print();

        try{
            env.execute("StreamKafka");
        }
        catch (Exception e){
            System.out.println(e.getMessage());

        }



    }



}

class RelevantTerms implements FilterFunction< Tuple2<String,Integer>>
{
    private ArrayList<String> relevantTerms;
    RelevantTerms(String path){
        relevantTerms = new ArrayList<>();
        addTermsFromFile(path);
        //I'm adding terms manually for now
        relevantTerms.add("tellico");
        relevantTerms.add("ocoee");
        relevantTerms.add("clearwater");
        relevantTerms.add("Video");

    }
    public boolean filter(Tuple2<String,Integer> theTerm){
        String s = theTerm.getField(0) ;
        return relevantTerms.contains(s);

    }
    private void addTermsFromFile(String filePath){
        //TODO IMPLEMENT

    }




}

class TokenizeTerms implements FlatMapFunction<String, Tuple2 <String, Integer>> {
    public void flatMap(String s, Collector<Tuple2<String,Integer>> out){
        String words[] = s.split("\\s+");
        for(String word: words){
            Tuple2<String,Integer> theT = new Tuple2<String,Integer>(word,1);
            out.collect(theT);
        }

    }

}
class StatusMessage implements MapFunction<FacebookPost,String>{
    public String map(FacebookPost thePost){
        String s = thePost.getStatusMessage().toString();
        return tokenize(s);
    }
    private String tokenize(String sentence) {
        StringBuffer s = new StringBuffer();
        for (int i = 0; i < sentence.length(); i++) {
            char c = sentence.charAt(i);
            if (Character.isLetterOrDigit(c)) {
                s.append(c);
            } else if (c == ' ' || c == '\t' || c == '\r') {
                s.append(' ');
            } else {
                //s.append(" " + c + " ");// pad symbol with space
            }
        }

        return s.toString().toLowerCase();
    }
}
class NLP implements MapFunction<String, String>{
    public String map( String message){
        Annotation a = new Annotation(message);
        List<CoreMap> sentences = a.get(CoreAnnotations.SentencesAnnotation.class);

        if(sentences !=null && !sentences.isEmpty()){
            return " the string is " + sentences.get(0).get(SentimentCoreAnnotations.SentimentClass.class);
        }
        return "stupid thing found nothing " ;
    }

}
