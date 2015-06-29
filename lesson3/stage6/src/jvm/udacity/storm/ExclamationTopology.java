package udacity.storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.testing.TestWordSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.util.HashMap;
import java.util.Map;

import udacity.storm.spout.*;
//******* Import MyLikesSpout and MyNamesSpout



/**
 * This is a basic example of a storm topology.
 *
 * This topology demonstrates how to add three exclamation marks '!!!'
 * to each word emitted
 *
 * This is an example for Udacity Real Time Analytics Course - ud381
 *
 */
public class ExclamationTopology {

  /**
   * A bolt that adds the exclamation marks '!!!' to word
   */
  public static class ExclamationBolt extends BaseRichBolt
  {
    // To output tuples from this bolt to the next stage bolts, if any
    OutputCollector _collector;
    private HashMap<String,String> favMap ;

    @Override
    public void prepare(
        Map                     map,
        TopologyContext         topologyContext,
        OutputCollector         collector)
    {
      // save the output collector for emitting tuples
      _collector = collector;
      favMap = new HashMap<String,String>();
    }

    @Override
    public void execute(Tuple tuple)
    {
      //**** ADD COMPONENT ID

      /*
       * Use component id to modify behavior
       */
       if (tuple.getSourceComponent().equals("my-like")) {
         String word = tuple.getString(0);
         String[] t = word.split("#");
         favMap.put(tuple.getString(0),tuple.getString(1));
       }
       if (tuple.getSourceComponent().equals("my-name")) {
         String word = tuple.getString(0);
         if (favMap.containsKey(word)) {
           StringBuilder exclamatedWord = new StringBuilder();
           exclamatedWord.append(word + "favorite is " +favMap.get(word)).append("!!!");
           _collector.emit(tuple, new Values(exclamatedWord.toString()));
         }
       }
      // get the column word from tuple
      String word = tuple.getString(0);

      // build the word with the exclamation marks appended


      // emit the word with exclamations

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
      // tell storm the schema of the output tuple for this spout

      // tuple consists of a single column called 'exclamated-word'
      declarer.declare(new Fields("exclamated-word"));
    }
  }

  public static void main(String[] args) throws Exception
  {
    // create the topology
    TopologyBuilder builder = new TopologyBuilder();

    // attach the word spout to the topology - parallelism of 10
    //builder.setSpout("word", new TestWordSpout(), 10);
    builder.setSpout("my-name", new MyNamesSpout(), 10);
    builder.setSpout("my-like", new MyLikesSpout(), 10);

    // attach the exclamation bolt to the topology - parallelism of 3
    builder.setBolt("exclaim1", new ExclamationBolt(), 3)
                .shuffleGrouping("my-like")
                .shuffleGrouping("my-name");

    // attach another exclamation bolt to the topology - parallelism of 2
    builder.setBolt("exclaim2", new ExclamationBolt(), 2).shuffleGrouping("exclaim1");

    builder.setBolt("report-bolt", new ReportBolt(),1).shuffleGrouping("exclaim1");

    // create the default config object
    Config conf = new Config();

    // set the config in debugging mode
    conf.setDebug(true);

    if (args != null && args.length > 0) {

      // run it in a live cluster

      // set the number of workers for running all spout and bolt tasks
      conf.setNumWorkers(3);

      // create the topology and submit with config
      StormSubmitter.submitTopology(args[0], conf, builder.createTopology());

    } else {

      // run it in a simulated local cluster

      // create the local cluster instance
      LocalCluster cluster = new LocalCluster();

      // submit the topology to the local cluster
      cluster.submitTopology("exclamation", conf, builder.createTopology());

      // let the topology run for 30 seconds. note topologies never terminate!
      Thread.sleep(30000);

      // kill the topology
      cluster.killTopology("exclamation");

      // we are done, so shutdown the local cluster
      cluster.shutdown();
    }
  }
}
