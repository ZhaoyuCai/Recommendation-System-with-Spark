/* 
 *  Movie Recommendation System 
 *
 *  PageRank Algorithm
 *
 *  Spark/Scala
 *
 *  Created on: Dec 10, 2016
 *  Last modified: Dec 15, 2016
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

def norating(x: MatchData) = {
	val user = x.user
	val movie = x.movie
	UMpair(user,movie)
} //eliminate attribute rating

val UMpair = lines.map(line => parse(line)).filter(x => x.rating>3).map(x =>(x.user,x.movie)).groupByKey().mapValues(_.toList).sortByKey()
val MUpair = lines.map(line => parse(line)).filter(x => x.rating>3).map(x =>(x.movie,x.user)).groupByKey().mapValues(_.toList).sortByKey()

user_j = 1 //change to a specific user for recommendation
val users_j = Array(1,10,15,20,100) //choose users
var user_j = 0

for (j <- 0 to 5){
	//Initialization
	var Similarity = UMpair.mapValues(v => 0.0).sortByKey() //Pair(userId,0.0)
	val temp = 1.0/MUpair.count
	var Relevance = MUpair.mapValues(v => temp).sortByKey() //Pair(movieId,temp)
	user_j = users_j(j)
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
	println("The recommend 20 new movies for user " + user_j + " are: " + recommend.mkString(", "))
	
}
