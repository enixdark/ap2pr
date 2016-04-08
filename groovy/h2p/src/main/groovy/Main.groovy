import org.apache.hadoop.util.ToolRunner
import com.demo.avro.AvroParse
import com.demo.mapreduce.WordCountConfigure

class Main {

	static def avroParse(){
		AvroParse.genericRecord()
	}
	static def mapreduceProcess(String...args){
		int exit = ToolRunner.run(new WordCountConfigure(), args)
		System.exit(exit)
	}
	static main(args) {
		 mapreduceProcess("/tmp/hdfs/tweets.txt","ouput")
	}

}
