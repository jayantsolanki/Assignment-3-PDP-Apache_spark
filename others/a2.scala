/*  YOUR_FIRST_NAME: Jayant
 *  YOUR_LAST_NAME:	Solanki
 *  YOUR_UBIT_NAME: jayantso
 */
/*
* Took references from:
*	1 - https://www.linkedin.com/pulse/connected-component-using-map-reduce-apache-spark-shirish-kumar
*	2 - http://mmds-data.org/presentations/2014/vassilvitskii_mmds14.pdf
*	Please check the build.sbt file for scala version check before proceeding to execute this file
*	Output graph is written inside the output folder at same directory level
*	Do delete the output folder using make clean before proceeding to execute
*	Build system is provided, just type make demo to run the code on given input.txt file
*/

// import org.apache.spark.graphx.GraphLoader
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

object a2 {
	def main(args: Array[String]) {
		val conf = new SparkConf().setAppName("Connected Components using Graphx")
		val sc = new SparkContext(conf)
		val MaxIterations: Long = 100 //set max limit for iterations here
		if (args.length != 2) {//arguments needed
			println("Two parameters are required, input file and output folder name")
			return
		}
		val input = args(0)
		val output = args(1)
		val graph = sc.textFile(input)//reading the graph from the input.txt
		// val verticesRDD = graph.map(line=>{
		// 	val vertices = line.split(" ")
		// 	(vertices(0), vertices(1))
		// })
		val verticesRDD = graph.map(line=>{
			val vertices = line.split(" ")
			(vertices(0).toLong, vertices(1).toLong)//converting to Long format, assuming the nodes will be in integers format
		})
		val originalGraph = verticesRDD.map(x => {
			x._1 + " " + x._2
		})//original graph in prescribed format

		///////---------Finding unique number of nodes in the graph----------///////
		val Originalx:RDD[Long] = verticesRDD.map(identity => {identity._1})
		val Originaly:RDD[Long] = verticesRDD.map(identity => {identity._2})
		val countOriginal = Originalx.union(Originaly).distinct.count()//finding the count of unique vertices in the graph, will be used for checking the convergence

		val (outputGraph, currentIteration) = converge(verticesRDD, 0, MaxIterations, countOriginal)
		//converting the outputGraph into the prescribed format
		val disp = outputGraph.distinct.map(x => x._1 + " " + x._2)//removing brackets and commas from source and destinations vertices, and adding space in between them
		disp.saveAsTextFile(output)//saving the RDD to a output folder
		val outputgraph = disp.collect().mkString("\n")// making RDD come into existence

		val countUnique = disp.count();//count unique pairs in the output graph, here it is assumed that each unique node will point to minimum values neighbour if the graph is converged, just a crude assumption, can be wrong too.

		////////////-------------Comment below println line to view the output-------------------///////
		//println("\nOriginal Graph---\n"+originalGraph.collect().mkString("\n")+"\n\nConnected Components---\n"+outputgraph+"\n")
		if(countUnique == countOriginal)
			println("\nConvergence achieved at "+currentIteration+"/"+MaxIterations+" iterations\n")
		else
			println("\nConvergence not achieved after "+MaxIterations+" iterations\n")
	}
	/*
	*	Function to repeatedly perform the LargeStar and SmallStar operations on the input graph
	*
	*
	*/
	def converge(graph: RDD[(Long, Long)], iterationCount: Long, MaxIterations: Long,  countElements: Long): (RDD[(Long, Long)], Long) = {
		val countUnique = graph.distinct.count()//count the unique pairs of vertices
		if(iterationCount == MaxIterations)//change this value for configuring the max iterations
			(graph, iterationCount)
		else if(countUnique == countElements)//Repeat Until Convergence, assumption here is that if the graph is converged than the the total disitnct pair count will be same as that of the number of unique nodes i.e., countElements
			(graph, iterationCount)
		else{
			val nodePairsLargeStar = largeStar(graph)
			val nodePairsSmallStar = smallStar(nodePairsLargeStar)
			converge(nodePairsSmallStar, iterationCount+1, MaxIterations, countElements)//this has to be done so as to prevent creation of immutable RDD of different names
		}
	}
	/*
	*	Function to SmallStar operation on the input graph
	*
	*
	*/
	def smallStar(graph: RDD[(Long, Long)]): (RDD[(Long, Long)]) = {
	/////////////////////---------First Step: Map----------//////////////////////////
		val emit = graph.map(x => {
			if (x._1 >= x._2)
				(x._1, x._2)
			else
				(x._2, x._1)
		})
		val neighbors = emit.map(x => {
			(x._1, List(x._2))
		})
	//////////////////////--------end of first step-------//////////////////////////
	//////////////////////-------------Second Step: Reduce-------------/////////////
		val allNeighbors = neighbors.reduceByKey(_.union(_))//creating the adjacency list for the vertex neighbours so as to find the neighbour with minimum value
		val vertexReConnection = allNeighbors.map(ver => {//visiting each node one by one and performing operations on its neighbours
			val currentnode = ver._1
			val currentnodeNeighbours = ver._2
			val minNeighbour = (currentnode :: currentnodeNeighbours).min //look for minimum neighbour including the current node too
			val reconnect = (currentnode :: currentnodeNeighbours).map(neighbors => {//switch the connections of all neighbours to minimum valued neighbour for the current node
				neighbors -> minNeighbour//assigning minimum valued neighbour to each neighbour of current Node, including the current node itself
				//emit list of neighbours and minNeighbour found including the current Node
			}).filter(x => {//applying the filter on the emitted list
				((x._1 <= currentnode && x._1 != x._2) || (currentnode == x._1))
			})
			(reconnect)//return only those vertices pairs which satisfy the vertex criteria of the smallstar
		})
		val newNodePairs = vertexReConnection.flatMap(x => x)//splitting the lists of list and getting the normal graph
		return(newNodePairs)
		//////////////////////--------end of Second step-------//////////////////////////
	}
	/*
	*	Function to LargeStar operation on the input graph
	*
	*
	*/
	def largeStar(graph: RDD[(Long, Long)]): (RDD[(Long, Long)]) = {
		/////////////////////---------First Step: Map----------//////////////////////////
		val emit = graph.flatMap(vertices => {//emitting the (u,v) and (v,u)//immutable
			if (vertices._1 == vertices._2)//prevent vertex pointing to itself emitted twice
				Seq((vertices._1, vertices._1))//converting vertices tuple back to list, multiple inputs
			else
				Seq((vertices._1, vertices._2), (vertices._2, vertices._1))
		})
		val neighbors = emit.map(x => {
			(x._1, List(x._2))
		})
		//////////////////////--------end of first step-------//////////////////////////
		//////////////////////-------------Second Step: Reduce-------------/////////////
		val allNeighbors = neighbors.reduceByKey(_.union(_))//creating the adjacency list for the vertex neighbours so as to find the neighbour with minimum value
		val vertexReConnection = allNeighbors.map(ver => {//visiting each node one by one and performing operations on its neighbours
			val currentnode = ver._1
			val currentnodeNeighbours = ver._2
			val minNeighbour = (currentnode :: currentnodeNeighbours).min //look for minimum neighbour including the current node too
			val reconnect = (currentnode :: currentnodeNeighbours).map(neighbors => {//switch the connections of all neighbours to minimum valued neighbour for the current node
				neighbors -> minNeighbour//assigning minimum valued neighbour to each neighbour of current Node, including the current node itself
				//emit list of neighbours and minNeighbour found including the current Node
			}).filter(x =>{//applying the filter on the emitted list
				((x._1 > currentnode && x._1 != x._2) || (currentnode == x._1))
			})
			(reconnect)//return only those vertices pairs which satisfy the vertex criteria of the largeStar
		})
		val newNodePairs = vertexReConnection.flatMap(x => x)//splitting the list of list and getting the normal graph
		return(newNodePairs)
		//////////////////////--------end of Second step-------//////////////////////////
	}

}