package de.ddm

import org.apache.spark.sql.{Dataset, KeyValueGroupedDataset, Row, SparkSession}


object Sindy {

  private def readData(input: String, spark: SparkSession): Dataset[Row] = {
    spark
      .read
      .option("inferSchema", "false")
      .option("header", "true")
      .option("quote", "\"")
      .option("delimiter", ";")
      .csv(input)
  }
  def generateCells(row:Row): Array[(String, String)]={
    val names = row.schema.names
    val result = new Array[(String,String)](row.length)
    for(i <- 0 until row.length){
      result(i) = (row.get(i).toString, names(i))
    }
    result
  }

  def generateInclusionList(list: List[String]):Array[(String,List[String])]={
    val result = new Array[(String,List[String])](list.length)
    for(i <- 0 until list.length){
      var resultList:List[String] = List()
      for(j <- 0 until list.length){
        if(i!=j){
          resultList = list(j) :: resultList
        }
      }
      result(i) = (list(i), resultList)
    }

    result
  }

  def listToString(array: List[String]):String={
    var result = ""
    for(s <- array) result += s + ", "
    result
  }

  def discoverINDs(inputs: List[String], spark: SparkSession): Unit = {
    import spark.implicits._
    println(inputs.length)
    var flattenInputs = readData(inputs(0), spark)
      .flatMap(f => generateCells(f));

    var counter = 1;
    while(counter < inputs.length) {
      val flattenInput = readData(inputs(counter), spark)
        .flatMap(f => generateCells(f))
      flattenInputs = flattenInputs.union(flattenInput);
      counter += 1;
    };

    val result = flattenInputs
      .groupByKey(a => a._1)
      .mapGroups((s,i) => i.toList)
      .map(l => l.map(t=> (t._2)).distinct)
      .flatMap(l => generateInclusionList(l))
      .groupByKey(t => t._1)
      .reduceGroups( (u,v) => (u._1, u._2.intersect(v._2:List[String])))
      .map(u => u._2)
      .filter(u => u._2.length > 0)
      .map(u => (u._1, listToString(u._2)))
      .toDF("Column", "Included In")
      .sort("Column")
      .map(u => (u(0) + " < " + u(1).toString))
      .collect()

    //OUTPUT-----------
    println("RESULTS:")

    counter = 0
    for(l <- result) {
      counter = counter + l.count(_ == ',')
      println(l)
    }

    println("Total number of INDs: " + counter)
  }
}
