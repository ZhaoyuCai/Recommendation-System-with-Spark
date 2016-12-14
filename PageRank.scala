/* 
 *  Movie Recommendation System 
 *
 *  PageRank Algorithm
 *
 *  Spark/Scala
 *
 *  Created on: Dec 14, 2016
 *  Last modified: Dec 14, 2016
 *
 *  Author: Zhaoyu Cai
 *
 *      
 */

 /* data: ratings.csv
  *
  userId，movieId，rating，timestamp
  1，31，2.5，1260759144
  *
  */

sc
val lines = sc.textFile("hdfs://master:9000/rating/ratings.csv")
lines.cache()
lines.take(10).foreach(println) //check the data

def toDouble(s: String) = { if ("?".equals(s)) Double.NaN else s.toDouble } 

case class MatchData(user: Int, movie: Int, rating: Double)

def parse(line: String) = { 
	val pieces = line.split(',') 
	val user = pieces(0).toInt 
	val movie = pieces(1).toInt 
	val rating = pieces(2).toDouble
	MatchData(user, movie, rating) 
} 

case class UMpair(user: Int, movie: Int)

def norating(x: MatchData) = {
	val user = x.user
	val movie = x.movie
	UMpair(user,movie)
}

val UMpair = lines.map(line => parse(line)).filter(x => x.rating>3).map(x =>(x.user,x.movie)).groupByKey().mapValues(_.toList).sortByKey()
val MUpair = lines.map(line => parse(line)).filter(x => x.rating>3).map(x =>(x.movie,x.user)).groupByKey().mapValues(_.toList).sortByKey()

//Initialization
var Similarity = UMpair.mapValues(v => 0.0).sortByKey()
val temp = 1.0/MUpair.count
var Relevance = MUpair.mapValues(v => temp).sortByKey()

user_j = 1 //change to a specific user for recommendation

for (i <- 1 to 30) {
	// Iterating Similarity
	val SimilarityContribs = MUpair.join(Relevance).values.flatMap{ case (users,relevance) =>
		val size = users.size
		users.map(user => (user, relevance/size))
	}
	Similarity = SimilarityContribs.reduceByKey(_ + _).sortByKey().map{ case (user,simi) =>
		if(user==user_j) (user,0.8*simi+0.2)
		else (user,0.8*simi)
	}

	// Iterating Relevance
	val RelevanceContribs = UMpair.join(Similarity).values.flatMap{ case (movies,similarity) =>
		val size = movies.size
		movies.map(movie => (movie, similarity/size))
	}
	Relevance = RelevanceContribs.reduceByKey(_ + _).sortByKey()
}

val alreadySeen = lines.map(line => parse(line)).filter(x => x.user == user_j).map(x =>x.movie).collect()
val waitList = Relevance.map{case(movie,rel) => (rel, movie)}.sortByKey(false).map{case(rel,movie) => (movie)}
val recommend = waitList.filter(!alreadySeen.contains(_)).take(20) //recommend 20 unseen movies
