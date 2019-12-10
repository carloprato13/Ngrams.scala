
import org.apache.spark.sql.SparkSession
import java.io._

object Ngram {

	def tokenize(src: String) : Array[String] = {
		("<s> " ++ src ++ " </s>").toLowerCase.replaceAll("""[[^a-zA-Z]+&&[^'<>/]]""", " ")  
		.split(" ")
		.filter(_.nonEmpty)
		}

	def getClosestMatch( m : Map[List[String], Int], elem : List[String]): (Int,Int) = {
		if ( m.contains(elem) ) (m(elem), elem.length)
		else getClosestMatch(m, elem.take(elem.length-1))
		}
		//while( ! m.contains(elem) ){
		//  elem.take(elem.length-1)  
		//}
		//(m(elem), elem.length)

	// args should contain:
	//	- path to corpus
	//	- maximum size of ngrams
	//	- path to output

	def main(args: Array[String]): Unit = {

		val spark = SparkSession
			.builder()
			.appName("Ngrams generator")
			.master("local[*]")
			.getOrCreate()

		spark.sparkContext.setLogLevel("ERROR")

		if (args.length ==0) 
			println("Ngrams counter: \n Args should contain: \n - path to corpus \n - maximum size of ngrams \n - path to output ")
	  	else 
	  		try {
		        val input =  spark.sparkContext.textFile(args(0)) //open file in args

		        val words = input.map( tokenize(_).toList) // tokenize every sentence 

		        // val totWords = words.map(_.size).sum().toInt //count number of words in the document

		        val maxn : Int = args(1).toInt //size of ngrams

		        //val ngrams = (for(sentence <- words ; 
		        //                  ngramsInSentence <- 
		        //                    for( n <- 1 to maxn;  
		        //                       ngram <- sentence.sliding(n).toList)
		        //                       yield ngram)
		        //                  yield ngramsInSentence )

		        val ngrams = words.flatMap(sentence => List.range(1, maxn).map( n => sentence.sliding(n).toList).flatten )

		        //val ngramsCount = ngrams.groupBy(identity).mapValues(_.size ) //no MapReduce

		        val ngramsCount = ngrams.map( x => (x, 1) ).reduceByKey(_ + _).persist() //with MapReduce

		        val vocabularySize = ngramsCount.filter( _._1.length == 1 ).count()

		        val copy = ngramsCount.filter( l => l._1.length < maxn  ).collect().toMap

		        val ngramsProbability = ngramsCount 
		                                        .map{ case (k,v) if k.length > 1 => 
		                                              	def elem = getClosestMatch(copy, k.take(k.length-1))
		                                              	(k, math.pow(0.4, maxn - elem._2) * v.toDouble / elem._1 )
		                                              case (k,v) =>
		                                              	(k, math.pow(0.4, maxn - 1) * v.toDouble / vocabularySize )
		                                            }

		        ngramsProbability.repartition(1).saveAsTextFile(args(2))
	        
	      } catch {
	         case ex: FileNotFoundException =>{
	            println("Missing file exception")
	         }
	         
	         case ex: IOException => {
	            println("IO Exception")
	         }

	      }finally{
	      	spark.stop
			}
    }
}
