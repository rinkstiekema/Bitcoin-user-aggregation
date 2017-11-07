/**
  * Copyright 2016 ZuInnoTe (Jörn Franke) <zuinnote@gmail.com>
  *
  * Licensed under the Apache License, Version 2.0 (the "License");
  * you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  **/

package org.zuinnote.spark.bitcoin.example

import org.apache.hadoop.conf._
import org.apache.hadoop.io._
import org.apache.spark._
import org.apache.spark.sql._
import org.graphframes.GraphFrame
import org.zuinnote.hadoop.bitcoin.format.common._
import org.zuinnote.hadoop.bitcoin.format.mapreduce._

object SparkScalaBitcoinTransactionGraph {
  def myFunction [A,B] (transfers : List[(Set[String], String)]) : List[(List[String],String)] = {
    transfers.map{ case (src, target) => (transfers.map(_._1).filter(s => src.intersect(s).nonEmpty).flatten , target)}
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Spark-Scala-Graphx BitcoinTransaction Graph (hadoopcryptoledger)")
    conf.set( "spark.serializer", "org.apache.spark.serializer.KryoSerializer" )
    conf.set("spark.kryoserializer.buffer.max", "2047")
    val sc = new SparkContext(conf)
    val hadoopConf = new Configuration()
    hadoopConf.set("hadoopcryptoledger.bitcoinblockinputformat.filter.magic", "F9BEB4D9")
    jobTop5AddressInput(sc, hadoopConf, args(0), args(1))
    sc.stop()
  }

  def jobTop5AddressInput(sc: SparkContext, hadoopConf: Configuration, inputFile: String, outputFile: String): Unit = {
    val bitcoinBlocksRDD = sc.newAPIHadoopFile(inputFile, classOf[BitcoinBlockFileInputFormat], classOf[BytesWritable], classOf[BitcoinBlock], hadoopConf)
    // extract a tuple per transaction containing Bitcoin destination address, the input transaction hash, the input transaction output index, and the current transaction hash, the current transaction output index, a (generated) long identifier
    val bitcoinTransactionTuples = bitcoinBlocksRDD.sample(true, 0.01, 1).flatMap(hadoopKeyValueTuple => extractTransactionData(hadoopKeyValueTuple._2))
    // create the vertex (vertexId, Bitcoin destination address), keep in mind that the flat table contains the same bitcoin address several times
    val bitcoinAddressIndexed = bitcoinTransactionTuples.map(bitcoinTransactions => bitcoinTransactions._1).distinct().zipWithIndex()
    // create the edges. Basically we need to determine which inputVertexId refers to which outputVertex Id.
    // This is basically a self join, where ((currentTransactionHash,currentOutputIndex), identfier) is joined with ((inputTransactionHash,currentinputIndex), indentifier)
    // a self join consumes a lot of resources, however there is no other way to create the graph
    // for production systems the following is recommended
    // 1) save bitcoinTransactionTuples to a file
    // 2) save the self-joined data to a file
    // 3) if new data arrives:
    // 	append new data bitcoinTransactionTuples and make sure that subsequent identifiers are generated for them
    //	get the generated identifiers for the new data together with the corresponding new data
    //	join the new data with bitcoinTransactionTuples
    //	append the joined data to the self-joined data in the file
    //	join the new vertices with the old graph in-memory
    // 	rerun any algorithm
    // (bitcoinAddress,(byteArrayTransaction, TransactionIndex)
    val inputTransactionTuple = bitcoinTransactionTuples.map(bitcoinTransactions => (bitcoinTransactions._1, (new ByteArray(bitcoinTransactions._2), bitcoinTransactions._3)))
    // (bitcoinAddress,((byteArrayTransaction, TransactionIndex),vertexid))
    val inputTransactionTupleWithIndex = inputTransactionTuple.join(bitcoinAddressIndexed)
    // (byteArrayTransaction, TransactionIndex), (vertexid, bitcoinAddress)
    val inputTransactionTupleByHashIdx = inputTransactionTupleWithIndex.map(iTTuple => (iTTuple._2._1, (iTTuple._2._2, iTTuple._1)))
    val currentTransactionTuple = bitcoinTransactionTuples.map(bitcoinTransactions => (bitcoinTransactions._1, (new ByteArray(bitcoinTransactions._4), bitcoinTransactions._5)))
    val currentTransactionTupleWithIndex = currentTransactionTuple.join(bitcoinAddressIndexed)
    // (byteArrayTransaction, TransactionIndex), (vertexid, bitcoinAddress)
    val currentTransactionTupleByHashIdx = currentTransactionTupleWithIndex.map { cTTuple => (cTTuple._2._1, (cTTuple._2._2, cTTuple._1)) }
    // the join creates ((ByteArray, Idx), (srcIdx,srcAdress), (destIdx,destAddress)
    val joinedTransactions = inputTransactionTupleByHashIdx.join(currentTransactionTupleByHashIdx)

    //sc.setCheckpointDir("hdfs://hathi-surfsara/user/lsde07/bitcoin/output")
    sc.setCheckpointDir("temp")
    val spark = new SparkSession.Builder().getOrCreate()
    import spark.implicits._


    joinedTransactions.cache()
    val aggregatedTransactions = myFunction(joinedTransactions.groupByKey().map(f => (f._2.map(x => x._1._2), f._2.head._2._2)).map(f => (f._1.toSet, f._2)).collect().toList)
    val aggregatedTransactionsFinal = aggregatedTransactions.map(f => {
      if(aggregatedTransactions.exists(x => x._1.contains(f._2))) {
        (f._1, aggregatedTransactions.filter(x => x._1.contains(f._2)).head._1)
      } else {
        (f._1, List(f._2))
      }
    })

    val count1 = joinedTransactions.map(f => f._2._1).distinct()
    val count2 = joinedTransactions.map(f => f._2._2).distinct()
    val distinctLeft = aggregatedTransactionsFinal.map(f => f._1)
    val distinctRight = aggregatedTransactionsFinal.map(f => f._2)
    val distinctBoth = distinctLeft.union(distinctRight).distinct.zipWithIndex
    sc.parallelize(distinctBoth).toDF("src", "id").repartition(1).write.mode("append").json("bitcoin/map")
    sc.parallelize(Seq(count1.union(count2).distinct().count(), distinctBoth.size)).repartition(1).saveAsTextFile("bitcoin/count")

    val aggregatedFinal = aggregatedTransactionsFinal.map(f => (distinctBoth.filter(x => x._1 == f._1).head._2, distinctBoth.filter(x => x._1 == f._2).head._2)).toDF("src","dst")
    val graphFrame: GraphFrame = GraphFrame.fromEdges(aggregatedFinal)
    val result = graphFrame.pageRank.resetProbability(0.15).tol(0.01).run()
    graphFrame.edges.repartition(1).write.mode("append").json("bitcoin/outputEdges")
    graphFrame.vertices.repartition(1).write.mode("append").json("bitcoin/outputVertices")

    result.edges.groupBy("src").count().repartition(1).write.mode("append").json("bitcoin/pageRank/outputEdges")
    result.vertices.repartition(1).write.mode("append").json("bitcoin/pageRank/outputVertices")
  }

  // extract relevant data
  def extractTransactionData(bitcoinBlock: BitcoinBlock): Array[(String, Array[Byte], Long, Array[Byte], Long)] = {
    // first we need to determine the size of the result set by calculating the total number of inputs multiplied by the outputs of each transaction in the block
    val transactionCount = bitcoinBlock.getTransactions().size()
    var resultSize = 0
    for (i <- 0 to transactionCount - 1) {
      resultSize += bitcoinBlock.getTransactions().get(i).getListOfInputs().size() * bitcoinBlock.getTransactions().get(i).getListOfOutputs().size()
    }

    // then we can create a tuple for each transaction input: Destination Address (which can be found in the output!), Input Transaction Hash, Current Transaction Hash, Current Transaction Output
    // as you can see there is no 1:1 or 1:n mapping from input to output in the Bitcoin blockchain, but n:m (all inputs are assigned to all outputs), cf. https://en.bitcoin.it/wiki/From_address
    val result: Array[(String, Array[Byte], Long, Array[Byte], Long)] = new Array[(String, Array[Byte], Long, Array[Byte], Long)](resultSize)
    var resultCounter: Int = 0
    for (i <- 0 to transactionCount - 1) {
      // for each transaction
      val currentTransaction = bitcoinBlock.getTransactions().get(i)
      val currentTransactionHash = BitcoinUtil.getTransactionHash(currentTransaction)
      for (j <- 0 to currentTransaction.getListOfInputs().size() - 1) {
        // for each input
        val currentTransactionInput = currentTransaction.getListOfInputs().get(j)
        val currentTransactionInputHash = currentTransactionInput.getPrevTransactionHash()
        val currentTransactionInputOutputIndex = currentTransactionInput.getPreviousTxOutIndex()
        for (k <- 0 to currentTransaction.getListOfOutputs().size() - 1) {
          val currentTransactionOutput = currentTransaction.getListOfOutputs().get(k)
          var currentTransactionOutputIndex = k.toLong
          result(resultCounter) = (BitcoinScriptPatternParser.getPaymentDestination(currentTransactionOutput.getTxOutScript()), currentTransactionInputHash, currentTransactionInputOutputIndex, currentTransactionHash, currentTransactionOutputIndex)
          resultCounter += 1
        }
      }

    }
    result
  }
}


/**
  * Helper class to make byte arrays comparable
  *
  *
  */
class ByteArray(val bArray: Array[Byte]) extends Serializable {
  override val hashCode = bArray.deep.hashCode

  override def equals(obj: Any) = obj.isInstanceOf[ByteArray] && obj.asInstanceOf[ByteArray].bArray.deep == this.bArray.deep
}
