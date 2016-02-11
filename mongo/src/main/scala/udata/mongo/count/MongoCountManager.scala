package udata.mongo.count

import akka.actor.Actor
import akka.pattern.pipe

import reactivemongo.api.collections.bson.BSONCollection
import reactivemongo.api.commands.GetLastError
import reactivemongo.api.indexes.{IndexType, Index}
import reactivemongo.bson._
import reactivemongo.core.commands.RawCommand

import udata.count.CountManager.{ResourceCountResponse, ResourceCountRequest, UpdateResponse, UpdateCountRequest}

import scala.concurrent.Future
import scala.concurrent.duration._


object MongoCountManager {

  case object CleanExpiredCounts

}

trait MongoCountManager extends Actor {

  import context.dispatcher
  import MongoCountManager._

  def mongoCollection: BSONCollection
  mongoCollection.indexesManager.ensure(Index(background = true, unique = true, key = List(("replaceKey", IndexType.Ascending))))
  mongoCollection.indexesManager.ensure(Index(background = true, unique = false, key = List(("resourceKey", IndexType.Ascending))))

  self ! CleanExpiredCounts



  def receive = {
    case UpdateCountRequest(resourceKey, count, ttl, replaceKey) =>
      val recipient = sender
      update(resourceKey, count, ttl, replaceKey).pipeTo(recipient)
    case ResourceCountRequest(resourceKey) =>
      val recipient = sender
      findCount(resourceKey).map{ ResourceCountResponse(_) } pipeTo(recipient)
    case CleanExpiredCounts => clean()
  }

  def update(resourceKey: String, amount: Int, ttl: FiniteDuration, replaceKey: Option[String] = None): Future[UpdateResponse] = {
    val newReplaceKey = java.util.UUID.randomUUID.toString
    val update = BSONDocument(
      "expiration" -> (System.currentTimeMillis + ttl.toMillis),
      "ttl" -> ttl.toMillis,
      "resourceKey" -> resourceKey,
      "replaceKey" -> newReplaceKey,
      "count" -> amount
    )
    val insert = replaceKey match {
      case Some(key) => mongoCollection.update(BSONDocument("replaceKey" -> key), update, writeConcern = GetLastError.Acknowledged, upsert = true, multi = false)
      case _ => mongoCollection.insert(update, writeConcern = GetLastError.Acknowledged)
    }
    insert.flatMap { x =>
      findCount(resourceKey)
    }.map { ct =>
      UpdateResponse(resourceKey, newReplaceKey, ct)
    }
  }

  def findCount(key: String) : Future[Int] = {
    val command = BSONDocument(
      "aggregate" -> mongoCollection.name,
      "pipeline" -> BSONArray(
        BSONDocument("$match" -> BSONDocument("resourceKey" -> key, "expiration" -> BSONDocument("$gt" -> System.currentTimeMillis))),
        BSONDocument(
          "$group" -> BSONDocument(
            "_id" -> "$resourceKey",
            "count" -> BSONDocument("$sum" -> "$count")
          )
        )
      )
    )


    val futureResult = mongoCollection.db.command(RawCommand(command))
    futureResult.map { case x =>
      val count = x.get("result").get.asInstanceOf[BSONArray].get(0).get.asInstanceOf[BSONDocument].get("count").get.asInstanceOf[BSONInteger].value
      count
    }.recoverWith { case x =>
      Future.successful(0)
    }
  }

  private def clean() {
    val command = BSONDocument("expiration" -> BSONDocument("$lte" -> System.currentTimeMillis))
    mongoCollection.remove(command, firstMatchOnly = false).onComplete { case _ =>
      context.system.scheduler.scheduleOnce(10.seconds) {
        self ! CleanExpiredCounts
      }
    }
  }
}
