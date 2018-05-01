package stackoverflow

import org.apache.spark.rdd.RDD

import scala.annotation.tailrec

/** The parsing and kmeans methods */
class dfdf extends Serializable {

  /** Languages */
  val langs =
    List(
      "JavaScript", "Java", "PHP", "Python", "C#", "C++", "Ruby", "CSS",
      "Objective-C", "Perl", "Scala", "Haskell", "MATLAB", "Clojure", "Groovy")

  /** K-means parameter: How "far apart" languages should be for the kmeans algorithm? */
  def langSpread = 50000
  assert(langSpread > 0, "If langSpread is zero we can't recover the language from the input data!")

  /** K-means parameter: Number of clusters */
  def kmeansKernels = 45

  /** K-means parameter: Convergence criteria */
  def kmeansEta: Double = 20.0D

  /** K-means parameter: Maximum iterations */
  def kmeansMaxIterations = 120


  //
  //
  // Parsing utilities:
  //
  //

  /** Load postings from the given file */
  def rawPostings(lines: RDD[String]): RDD[Posting] =
    lines.map(line => {
      val arr = line.split(",")
      Posting(postingType =    arr(0).toInt,
        id =             arr(1).toInt,
        acceptedAnswer = if (arr(2) == "") None else Some(arr(2).toInt), // use Some() if the value is optional
        parentId =       if (arr(3) == "") None else Some(arr(3).toInt),
        score =          arr(4).toInt,
        tags =           if (arr.length >= 6) Some(arr(5).intern()) else None)
    })


  /** Group the questions and answers together */
  def groupedPostings(postings: RDD[Posting]): RDD[(QID, Iterable[(Question, Answer)])] = {
    val questions = postings.filter(posting => posting.postingType==1).map(p => (p.id,p))
    val answers = postings.filter(posting => posting.postingType==2).map(p => (p.parentId.get,p)) // we used get here because this variable is optional
    questions.join(answers).groupByKey()
  }


  /** Compute the maximum score for each posting */
  def scoredPostings(grouped: RDD[(QID, Iterable[(Question, Answer)])]): RDD[(Question, HighScore)] = {
  // def scoredPostings(grouped: RDD[(Int, Iterable[(Posting, Posting)])]): RDD[(Posting, Int)] = { as an alternative

    def answerHighScore(as: Array[Answer]): HighScore = {
      var highScore = 0
      var i = 0
      while (i < as.length) {
        val score = as(i).score
        if (score > highScore)
          highScore = score
        i += 1
      }
      highScore
    }
    // Avant values :
    // (QID1, [(Q1, A1), (Q1, A2), (Q1, A3)])
    // Après values :
    // x : [(Q1, A1), (Q1, A2), (Q1, A3)]
    // x.head : (Q1, A1)
    // x.head._1 : Q1
    // x.map(_) returns RDD : ((Q1,A1), (Q1,A2), (Q1,A3)) but after grouped.values
    // x.map(_._2) returns RDD : (A1, A2, A3)

    // we used .values to get rid of the keys, we cannot get rid of the keys by using mapValues()
    grouped.values.map(x => (x.head._1, answerHighScore(x.map(_._2).toArray))) // we used .head to get the first element of the iterable since all the questions are the same in all the grouped tuples then we put ._1 to get only hte questions part of the tuple
    //grouped.flatMap(x => x._2).groupByKey().map(x => (x._1,answerHighScore(x._2.toArray))) //groupByKey creates a shuffling problem which is good for the performance
  }


  /** Compute the vectors for the kmeans */
  def vectorPostings(scored: RDD[(Question, HighScore)]): RDD[(LangIndex, HighScore)] = {
    /** Return optional index of first language that occurs in `tags`. */
    def firstLangInTag(tag: Option[String], ls: List[String]): Option[Int] = {
      if (tag.isEmpty) None
      else if (ls.isEmpty) None
      else if (tag.get == ls.head) Some(0) // index: 0
      else {
        val tmp = firstLangInTag(tag, ls.tail)
        tmp match {
          case None => None
          case Some(i) => Some(i + 1) // index i in ls.tail => index i+1
        }
      }
    }

    scored.map(x => (firstLangInTag(x._1.tags,langs).get * langSpread, x._2)).cache()
    //.cache() // we added cache() here because the kmeans usees vectors variable multiple times (recursively)//if .persist(StorageLevel. ...)
    // we used get to obtain the returned Int value of the function firstLangInTag
  }


  /** Sample the vectors */
  def sampleVectors(vectors: RDD[(LangIndex, HighScore)]): Array[(Int, Int)] = {

    assert(kmeansKernels % langs.length == 0, "kmeansKernels should be a multiple of the number of languages studied.")
    val perLang = kmeansKernels / langs.length

    // http://en.wikipedia.org/wiki/Reservoir_sampling
    def reservoirSampling(lang: Int, iter: Iterator[Int], size: Int): Array[Int] = {
      val res = new Array[Int](size)
      val rnd = new util.Random(lang)

      for (i <- 0 until size) {
        assert(iter.hasNext, s"iterator must have at least $size elements")
        res(i) = iter.next
      }

      var i = size.toLong
      while (iter.hasNext) {
        val elt = iter.next
        val j = math.abs(rnd.nextLong) % i
        if (j < size)
          res(j.toInt) = elt
        i += 1
      }

      res
    }

    val res =
      if (langSpread < 500)
      // sample the space regardless of the language
        vectors.takeSample(false, kmeansKernels, 42)
      else
      // sample the space uniformly from each language partition
        vectors.groupByKey.flatMap({
          case (lang, vectors) => reservoirSampling(lang, vectors.toIterator, perLang).map((lang, _))
        }).collect()

    assert(res.length == kmeansKernels, res.length)
    res
  }


  //
  //
  //  Kmeans method:
  //
  //

  /** Main kmeans computation */
  @tailrec final def kmeans(means: Array[(Int, Int)], vectors: RDD[(Int, Int)], iter: Int = 1, debug: Boolean = false): Array[(Int, Int)] = {
    val newMeans = means.clone() // you need to compute newMeans

    // TODO: Fill in the newMeans array
    // x
    val temp = vectors.map(x => (findClosest(x,means), x)).groupByKey().mapValues(x => averageVectors(x)).collect()
                                                                      .foreach(t => newMeans.update(t._1,t._2)) // to update the centers
    val distance = euclideanDistance(means, newMeans)

    if (debug) {
      println(s"""Iteration: $iter
                 |  * current distance: $distance
                 |  * desired distance: $kmeansEta
                 |  * means:""".stripMargin)
      for (idx <- 0 until kmeansKernels)
        println(f"   ${means(idx).toString}%20s ==> ${newMeans(idx).toString}%20s  " +
          f"  distance: ${euclideanDistance(means(idx), newMeans(idx))}%8.0f")
    }

    if (converged(distance))
      newMeans
    else if (iter < kmeansMaxIterations)
      kmeans(newMeans, vectors, iter + 1, debug)
    else {
      if (debug) {
        println("Reached max iterations!")
      }
      newMeans
    }
  }




  //
  //
  //  Kmeans utilities:
  //
  //

  /** Decide whether the kmeans clustering converged */
  def converged(distance: Double) =
    distance < kmeansEta


  /** Return the euclidean distance between two points */
  def euclideanDistance(v1: (Int, Int), v2: (Int, Int)): Double = {
    val part1 = (v1._1 - v2._1).toDouble * (v1._1 - v2._1)
    val part2 = (v1._2 - v2._2).toDouble * (v1._2 - v2._2)
    part1 + part2
  }

  /** Return the euclidean distance between two points */
  def euclideanDistance(a1: Array[(Int, Int)], a2: Array[(Int, Int)]): Double = {
    assert(a1.length == a2.length)
    var sum = 0d
    var idx = 0
    while(idx < a1.length) {
      sum += euclideanDistance(a1(idx), a2(idx))
      idx += 1
    }
    sum
  }

  /** Return the closest point */
  def findClosest(p: (Int, Int), centers: Array[(Int, Int)]): Int = {
    var bestIndex = 0
    var closest = Double.PositiveInfinity
    for (i <- 0 until centers.length) {
      val tempDist = euclideanDistance(p, centers(i))
      if (tempDist < closest) {
        closest = tempDist
        bestIndex = i
      }
    }
    bestIndex
  }


  /** Average the vectors */
  def averageVectors(ps: Iterable[(Int, Int)]): (Int, Int) = {
    val iter = ps.iterator
    var count = 0
    var comp1: Long = 0
    var comp2: Long = 0
    while (iter.hasNext) {
      val item = iter.next
      comp1 += item._1
      comp2 += item._2
      count += 1
    }
    ((comp1 / count).toInt, (comp2 / count).toInt)
  }

  // creating a median calculation method
  private[this] def medianCalculation(xs: List[Int]): Int = {
    val xsSorted = xs.sortWith(_ < _)
    val xsLen = xs.length
    if (xsLen % 2 != 0)
      xsSorted.drop(xsLen / 2).head
    else {
      val xsStart = xsLen / 2 - 1
      val xsSortedDropped = xsSorted.drop(xsStart)
      (xsSortedDropped.head + xsSortedDropped.tail.head) / 2
    }
  }

  //
  //
  //  Displaying results:
  //
  //
  def clusterResults(means: Array[(Int, Int)], vectors: RDD[(LangIndex, HighScore)]): Array[(String, Double, Int, Int)] = {
    val closest = vectors.map(p => (findClosest(p, means), p))
    val closestGrouped = closest.groupByKey()

    val median = closestGrouped.mapValues { vs =>
      val langLabel: String   = langs(vs.groupBy(_._1).maxBy(_._2.size)._1 / langSpread)// most common language in the cluster
      val langPercent: Double = (vs.groupBy(_._1).maxBy(_._2.size)._2.size.toDouble / vs.size.toDouble ) * 100.0d // vs.groupBy(_._1).flatMap(_._2).size // percent of the questions in the most common language
      // vs.groupBy(_._1).maxBy(_._2.size) to find the most common language
      // vs.groupBy(_._1).maxBy(_._2.size)._2.size to get the grouped highScores array's length
      // vs.size gives the all pairs in the cluster but each pair unique therefore it guves the total number of unique pairs
      val clusterSize: Int    = vs.size
      val medianScore: Int = medianCalculation(vs.map(_._2).toList)

      (langLabel, langPercent, clusterSize, medianScore)
    }

    median.collect().map(_._2).sortBy(_._4)
  }

  def printResults(results: Array[(String, Double, Int, Int)]): Unit = {
    println("Resulting clusters:")
    println("  Score  Dominant language (%percent)  Questions")
    println("================================================")
    for ((lang, percent, size, score) <- results)
      println(f"${score}%7d  ${lang}%-17s (${percent}%-5.1f%%) ${size}%7d")
  }
}