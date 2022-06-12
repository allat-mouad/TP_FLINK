import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.util.Collector;

public class Application1  {
    public static void main(String[] args) throws Exception {
// set up the execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
// get input data
        DataSet<String> text = env.readTextFile("input.txt");
        DataSet<Tuple2<String, Integer>> counts =
// split up the lines in pairs (2-tuples) containing: (word, 1)
        text.flatMap(new Tokenizer())
// group by the tuple field "0" and sum up tuple field "1"
                .groupBy(0)
                .sum(1);
// emit result

            System.out.println("Printing result to stdout. ");
            counts.print();

    }

    public static final class Tokenizer implements FlatMapFunction<String,
            Tuple2<String, Integer>> {
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            // normalize and split the line
            String[] tokens = value.toLowerCase().split("\\W+");
            // emit the pairs
            for (String token : tokens) {
                if (token.length() > 0) {
                    out.collect(new Tuple2<>(token, 1));
                }
            }
        }
    }
}
