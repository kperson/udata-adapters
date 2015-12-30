package udata.mongo.lock

import akka.actor.Actor
import akka.pattern.pipe

import reactivemongo.api.indexes.{IndexType, Index}
import reactivemongo.api.collections.bson.BSONCollection
import reactivemongo.bson._
import reactivemongo.core.commands.RawCommand

import scala.concurrent.{Future, Promise}
import scala.concurrent.duration._
import scala.util.Random
import scala.collection.mutable.{Map => MutableMap}

import udata.lock.LockManager._


object MongoLockManager {

  case object Monitor

}

trait MongoLockManager extends Actor {

  import MongoLockManager._
  import context.dispatcher

  def mongoCollection: BSONCollection

  mongoCollection.indexesManager.ensure(Index(background = true, unique = false, key = List(("resource", IndexType.Ascending))))
  mongoCollection.indexesManager.ensure(Index(background = true, unique = true, key = List(("lockId", IndexType.Ascending))))


  private val random = new Random
  private val watchedLocks:MutableMap[String, (String, Promise[LockAcquireResponse])] = MutableMap.empty

  self ! Monitor


  def receive = {
    case Monitor => monitor()
    case x @ LockAcquireRequest(resource, acquireTimeout, holdTimeout) =>
      val recipient = sender
      lock(resource, acquireTimeout, holdTimeout).pipeTo(recipient)
    case LockReleaseRequest(resource, auto) => unlock(resource)
  }


  def unlock(resource: String) {
    mongoCollection.remove(BSONDocument("resource" -> resource, "holdTimeout" -> BSONDocument("$ne" -> -1)))
  }

  def lock(resource: String, acquireTimeout: FiniteDuration, holdTimeout: FiniteDuration) : Future[LockAcquireResponse] =  {

    val lockId = java.util.UUID.randomUUID.toString

    val promise = Promise[LockAcquireResponse]()
    watchedLocks(lockId) = (resource, promise)
    context.system.scheduler.scheduleOnce(acquireTimeout) {
      if(!promise.isCompleted) {
        watchedLocks.remove(lockId)
        promise.success(LockTimeout(acquireTimeout))
      }
    }
    val insert = BSONDocument(
      "acquireTimeout" -> (System.currentTimeMillis + acquireTimeout.toMillis),
      "ttl" -> holdTimeout.toMillis,
      "resource" -> resource,
      "holdTimeout" -> -1,
      "createdAt" -> System.currentTimeMillis,
      "tiebreaker" -> random.nextInt,
      "lockId" -> lockId
    )
    mongoCollection.insert(insert)
    promise.future
  }

  private def lockKeys = {
    watchedLocks.keys.toList
  }

  private def resourceKeys = {
    watchedLocks.values.map(_._1).toList
  }


  private def monitor() {
    if(!lockKeys.isEmpty) {
      val currentTime = System.currentTimeMillis
      val deleteTimeout = BSONDocument("acquireTimeout" -> BSONDocument("$lte" -> currentTime))
      val deleteHoldExpiration = BSONDocument("$and" -> BSONArray(
        BSONDocument("holdTimeout" -> BSONDocument("$ne" -> -1)),
        BSONDocument("holdTimeout" -> BSONDocument("$lte" -> currentTime))
      ))
      val toAward = BSONDocument(
        "aggregate" -> mongoCollection.name,
        "pipeline" -> BSONArray(
          BSONDocument("$match" ->
            BSONDocument(
              "resource" -> BSONDocument("$in" -> resourceKeys),
              "acquireTimeout" -> BSONDocument("$gt" -> currentTime)
            )
          ),
          BSONDocument("$sort" -> BSONDocument("resource" -> 1, "holdTimeout" -> -1, "createdAt" -> 1, "tiebreaker" -> -1)),
          BSONDocument("$group" ->
            BSONDocument(
              "_id" -> "$resource",
              "lockId" -> BSONDocument("$first" -> "$lockId"),
              "holdTimeout" -> BSONDocument("$first" -> "$holdTimeout"),
              "ttl" -> BSONDocument("$first" -> "$ttl")
            )
          )
        )
      )
      mongoCollection.remove(BSONDocument("$or" -> BSONArray(deleteTimeout, deleteHoldExpiration))).onComplete { case _ =>
        val futureResult = mongoCollection.db.command(RawCommand(toAward))
        futureResult.onSuccess { case x =>
          val toAward = x.get("result").get.asInstanceOf[BSONArray]
          toAward.values.foreach {
            case x: BSONDocument => {
              val lockId = x.get("lockId").get.asInstanceOf[BSONString].value
              val ttl = x.get("ttl").get.asInstanceOf[BSONLong].value
              val updateSelector = BSONDocument("holdTimeout" -> -1, "lockId" -> lockId, "acquireTimeout" -> BSONDocument("$gt" -> System.currentTimeMillis))
              val updateWrite = BSONDocument("$set" -> BSONDocument("holdTimeout" -> (System.currentTimeMillis + ttl)))
              mongoCollection.findAndUpdate(updateSelector, updateWrite).onSuccess { case rs =>
                rs.lastError.foreach { _ =>
                  watchedLocks.remove(lockId).filter { case (_, x) => !x.isCompleted }.foreach { q =>
                    q._2.success(LockGrant(q._1))
                  }
                }
              }
            }
          }
        }
        futureResult.onComplete { case x =>
          Option(context).filter(!_.system.isTerminated).foreach { c =>
            c.system.scheduler.scheduleOnce(250.milliseconds) {
              self ! Monitor
            }
          }
        }
      }
    }
    else {
      Option(context).filter(!_.system.isTerminated).foreach { c =>
        c.system.scheduler.scheduleOnce(250.milliseconds) {
          self ! Monitor
        }
      }
    }
  }

}