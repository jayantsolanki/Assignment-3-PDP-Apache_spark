import org.apache.spark.graphx.GraphLoader
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf


object A2 {
 def main(args: Array[String]) {
 	val conf = new SparkConf().setAppName("Connected Components using Graphx")
    val sc = new SparkContext(conf)
    if (args.length != 2) {//arguments needed
        println("Two parameters are required, input file and output folder name")
        return
    }
    val input = args(0)
    val output = args(1)
	val graph = GraphLoader.edgeListFile(sc, input)//lading the graph from the input file
	//Finding the connected components, calling the inbuilt function
	val cc = graph.connectedComponents().vertices
	val disp = cc.map(x => x._1 + " " + x._2)//removing brackets and comma from source and destinations vertices, and adding space in between them
	val outputgraph = disp.collect().mkString("\n")// making RDD come into existence
	disp.saveAsTextFile(output)//saving the RDD to a output file
	println(outputgraph)//debug
   }
}