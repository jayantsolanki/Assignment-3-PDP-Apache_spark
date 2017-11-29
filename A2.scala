// import org.apache.spark.graphx.GraphLoader
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
		val graph = sc.textFile(input)//reading the graph
		val verticesRDD = graph.map(line=>{
			val vertices = line.split(" ")
			(vertices(0), vertices(1))
		})
		val disp = verticesRDD.map(x => {
			x._1 + " " + x._2
		})//original graph
		// val largeStar = verticesRDD.map(ver => {
		// 	(ver._1,ver._2)

		// })
		//////////////////large star/////////////////
		//Map
		val expandVer = verticesRDD.flatMap(vertices => {//emitting the (u,v) and (v,u)
			(vertices._1, vertices._2)
			if (vertices._1 == vertices._2)
				((vertices._1, vertices._1)).tolist
			else
				((vertices._1, vertices._2), (vertices._2, vertices._1)).tolist
		})
		//reduce
		// val neighbors
		// val minVertex = argMin
		println(expandVer.collect().mkString("\n"))
	}
	def argMin(nodes: List[Long]): Long = {
		nodes.min(Ordering.by((node: Long) => node))
	}
}