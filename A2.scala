/*
* Took references from:
*	1 - https://www.linkedin.com/pulse/connected-component-using-map-reduce-apache-spark-shirish-kumar
*	2 - http://mmds-data.org/presentations/2014/vassilvitskii_mmds14.pdf
*
*
*
*/
// import org.apache.spark.graphx.GraphLoader
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import scala.annotation.tailrec
import scala.collection.mutable.HashSet
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

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
		val graph = sc.textFile(input)//reading the graph from the input.txt
		// val verticesRDD = graph.map(line=>{
		// 	val vertices = line.split(" ")
		// 	(vertices(0), vertices(1))
		// })
		val verticesRDD = graph.map(line=>{
			val vertices = line.split(" ")
			(vertices(0).toLong, vertices(1).toLong)//converting to Long format, assuming the nodes will be in integers format
		})
		val disp = verticesRDD.map(x => {
			x._1 + " " + x._2
		})//original graph in formatted way
		val (outputGraph, count) = converge(verticesRDD, 0)
		// val (nodePairsLargeStar, _) = largeSStar(verticesRDD)
		// val (nodePairsSmallStar, _) = smallSStar(nodePairsLargeStar)
		// val (nodePairsLargeStar1, _) = largeSStar(nodePairsSmallStar)
		// val (nodePairsSmallStar1, _) = smallSStar(nodePairsLargeStar1)
		// val (nodePairsLargeStar2, _) = largeSStar(nodePairsSmallStar1)
		// val (nodePairsSmallStar2, _) = smallSStar(nodePairsLargeStar2)
		// val (nodePairsLargeStar3, _) = largeSStar(nodePairsSmallStar2)
		// val (nodePairsSmallStar3, _) = smallSStar(nodePairsLargeStar3)

		println("\nOriginal Graph ---\n"+verticesRDD.collect().mkString("\n")+"\n\nConnected Compnoents---\n"+outputGraph.distinct.collect().mkString("\n \nCount is "+count+"\n"))
	}
	def converge(graph: RDD[(Long, Long)], iterationCount: Int): (RDD[(Long, Long)], Int) = {

		if(iterationCount == 5)
			(graph, iterationCount)
		else{
			val newCount = iterationCount+1
			val (nodePairsLargeStar, _) = largeSStar(graph)
			val (nodePairsSmallStar, _) = smallSStar(nodePairsLargeStar)
			converge(nodePairsSmallStar, newCount)

		}

	}
	private def argMin(nodes: List[Long]): Long = {
		nodes.min(Ordering.by((node: Long) => node))
	}

	def smallSStar(graph: RDD[(Long, Long)]): (RDD[(Long, Long)], Int) = {
	/////////////////////---------First Step: Map----------//////////////////////////
		val emit = graph.map(x => {
			// val (self, neighbor) = (x._1, x._2)
			if (x._1 > x._2)
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
		val vertexReConnection = allNeighbors.map(ver => {
			val currentnode = ver._1
			val currentnodeNeighbours = ver._2
			// val currentnodeNeighbours = ver._2.distinct
			val minNode = (currentnode :: currentnodeNeighbours).min //look for minimum neighbour including the current node too
			val reconnect = (currentnode :: currentnodeNeighbours).map(neighbor => {
				(neighbor, minNode)
			}).filter(x =>{
				val neighbor = x._1
				val minNode = x._2
				((neighbor <= currentnode && neighbor != minNode) || (currentnode == neighbor))
			})
			(reconnect)
		})
		val newNodePairs = vertexReConnection.flatMap(x => x)//splitting the list of list and getting the normal graph
		// val newNode = vertexReConnection.toSet.toList
		// val graphs = graph.flatMap{
		// 	s => List((s._1, s._2),(s._2, s._1))
		// }.distinct().groupByKey()
		// val intermediate: RDD[(Long, Set[Long])] = neighbors.flatMap(v => hash(v._1, v._2))
		//finding the neighbours for each u
		// val allNeighbors = neighbors.reduceByKey(_.union(_))
		// print("hello neighbours \n"+newNodePairs.collect().mkString("\n"))
		return(newNodePairs, 0)
		//////////////////////--------end of Second step-------//////////////////////////
	}

	def largeSStar(graph: RDD[(Long, Long)]): (RDD[(Long, Long)], Int) = {
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
		val vertexReConnection = allNeighbors.map(ver => {
			val currentnode = ver._1
			val currentnodeNeighbours = ver._2
			val minNode = (currentnode :: currentnodeNeighbours).min //look for minimum neighbour including the current node too
			val reconnect = (currentnode :: currentnodeNeighbours).map(neighbor => {
				(neighbor, minNode)
			}).filter(x =>{
				val neighbor = x._1
				val minNode = x._2
				((neighbor > currentnode && neighbor != minNode) || (currentnode == neighbor))
			})
			val uniqueNewNodePairs = reconnect.toSet.toList
			(uniqueNewNodePairs)
		})
		val newNodePairs = vertexReConnection.flatMap(x => x)//splitting the list of list and getting the normal graph
		// // val newNode = vertexReConnection.toSet.toList
		// // val graphs = graph.flatMap{
		// // 	s => List((s._1, s._2),(s._2, s._1))
		// // }.distinct().groupByKey()
		// // val intermediate: RDD[(Long, Set[Long])] = neighbors.flatMap(v => hash(v._1, v._2))
		// //finding the neighbours for each u
		// // val allNeighbors = neighbors.reduceByKey(_.union(_))
		// print("hello large neighbours \n"+vertexReConnection.collect().mkString("\n"))
		return(newNodePairs, 0)
		//////////////////////--------end of Second step-------//////////////////////////
	}

	private def smallStar(graph: RDD[(Long, Long)]): (RDD[(Long, Long)], Int) = {

		/**
		  * generate RDD of (self, List(neighbors)) where self > neighbors
		  * E.g.: nodePairs (1, 4), (6, 1), (3, 2), (6, 5)
		  * will result into (4, List(1)), (6, List(1)), (3, List(2)), (6, List(5))
		  */
		val neighbors = graph.map(x => {
			// val (self, neighbor) = (x._1, x._2)
			if (x._1 > x._2)
				(x._1, List(x._2))
			else
				(x._2, List(x._1))
		})

		/**
		  * reduce on self to get list of all its neighbors.
		  * E.g: (4, List(1)), (6, List(1)), (3, List(2)), (6, List(5))
		  * will result into (4, List(1)), (6, List(1, 5)), (3, List(2))
		  * Note:
		  * (1) you may need to tweak number of partitions.
		  * (2) also, watch out for data skew. In that case, consider using rangePartitioner
		  */

		val allNeighbors = neighbors.reduceByKey((a, b) => {
			  b ::: a
		})

		/**
		  * Apply Small Star operation on (self, List(neighbor)) to get newNodePairs and count the change in connectivity
		  */

		val newNodePairsWithChangeCount = allNeighbors.map(x => {
		  val self = x._1
		  val neighbors = x._2.distinct
		  val minNode = argMin(self :: neighbors)
		  val newNodePairs = (self :: neighbors).map(neighbor => {
			(neighbor, minNode)
		  }).filter(x => {
			val neighbor = x._1
			val minNode = x._2
			((neighbor <= self && neighbor != minNode) || (self == neighbor))
		  })
		  val uniqueNewNodePairs = newNodePairs.toSet.toList

		  /**
			* We count the change by taking a diff of the new node pairs with the old node pairs
			*/
		  val connectivtyChangeCount = (uniqueNewNodePairs diff neighbors.map((self, _))).length
		  (uniqueNewNodePairs, connectivtyChangeCount)
		}).persist(StorageLevel.MEMORY_AND_DISK_SER)

		/**
		  * Sum all the changeCounts
		  */

		val totalConnectivityCountChange = newNodePairsWithChangeCount.mapPartitions(iter => {
			val (v, l) = iter.toSeq.unzip
			val sum = l.foldLeft(0)(_ + _)
			Iterator(sum)
		}).sum.toInt

		val newNodePairs = newNodePairsWithChangeCount.map(x => x._1).flatMap(x => x)
		newNodePairsWithChangeCount.unpersist(false)
		(newNodePairs, totalConnectivityCountChange)
	}

	private def largeStar(nodePairs: RDD[(Long, Long)]): (RDD[(Long, Long)], Int) = {

		/**
		  * generate RDD of (self, List(neighbors))
		  * E.g.: nodePairs (1, 4), (6, 1), (3, 2), (6, 5)
		  * will result into (4, List(1)), (1, List(4)), (6, List(1)), (1, List(6)), (3, List(2)), (2, List(3)), (6, List(5)), (5, List(6))
		  */

		val neighbors = nodePairs.flatMap(x => {
		  val (self, neighbor) = (x._1, x._2)
		  if (self == neighbor)
		    List((self, neighbor))
		  else
		    List((self, neighbor), (neighbor, self))
		})

		/**
		  * reduce on self to get list of all its neighbors.
		  * We are using aggregateByKey. You may choose to use reduceByKey as in the Small Star
		  * E.g: (4, List(1)), (1, List(4)), (6, List(1)), (1, List(6)), (3, List(2)), (2, List(3)), (6, List(5)), (5, List(6))
		  * will result into (4, List(1)), (1, List(4, 6)), (6, List(1, 5)), (3, List(2)), (2, List(3)), (5, List(6))
		  * Note:
		  * (1) you may need to tweak number of partitions.
		  * (2) also, watch out for data skew. In that case, consider using rangePartitioner
		  */

		val localAdd = (s: HashSet[Long], v: Long) => s += v
		val partitionAdd = (s1: HashSet[Long], s2: HashSet[Long]) => s1 ++= s2
		val allNeighbors = neighbors.aggregateByKey(HashSet.empty[Long]/*, rangePartitioner*/)(localAdd, partitionAdd)

		/**
		  * Apply Large Star operation on (self, List(neighbor)) to get newNodePairs and count the change in connectivity
		  */

		val newNodePairsWithChangeCount = allNeighbors.map(x => {
		  val self = x._1
		  val neighbors = x._2.toList
		  val minNode = argMin(self :: neighbors)
		  val newNodePairs = (self :: neighbors).map(neighbor => {
		    (neighbor, minNode)
		  }).filter(x => {
		    val neighbor = x._1
		    val minNode = x._2
		    ((neighbor > self && neighbor != minNode) || (neighbor == self))
		  })

		  val uniqueNewNodePairs = newNodePairs.toSet.toList
		  val connectivtyChangeCount = (uniqueNewNodePairs diff neighbors.map((self, _))).length
		  (uniqueNewNodePairs, connectivtyChangeCount)
		}).persist(StorageLevel.MEMORY_AND_DISK_SER)

		val totalConnectivityCountChange = newNodePairsWithChangeCount.mapPartitions(iter => {
		  val (v, l) = iter.toSeq.unzip
		  val sum = l.foldLeft(0)(_ + _)
		  Iterator(sum)
		}).sum.toInt

		/**
		  * Sum all the changeCounts
		  */
		val newNodePairs = newNodePairsWithChangeCount.map(x => x._1).flatMap(x => x)
		newNodePairsWithChangeCount.unpersist(false)
		(newNodePairs, totalConnectivityCountChange)
		return (newNodePairs, 0.toInt)
	}
}