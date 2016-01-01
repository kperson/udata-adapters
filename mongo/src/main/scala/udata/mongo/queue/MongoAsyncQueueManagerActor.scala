package udata.mongo.queue

import akka.actor.Actor

import reactivemongo.api.collections.bson.BSONCollection
import reactivemongo.api.indexes.{IndexType, Index}
import reactivemongo.bson.{BSONBinary, BSONString, BSONDocument}

import scala.concurrent.duration._
import scala.util.Random

import udata.queue.AsyncQueueManager
import udata.queue.AsyncQueueManagerActor._


object MongoAsyncQueueManagerActor {

  private[queue] case class LoadQueueSearch(max: Int)
  private[queue] case class LoadQueueRequest(key: String, bytes: Array[Byte])

  val loopMax = 50

}

trait MongoAsyncQueueManagerActor extends Actor {

  import MongoAsyncQueueManagerActor._
  import context.dispatcher

  //dependencies
  def dataCollection: BSONCollection
  def manager:AsyncQueueManager[Array[Byte]]

  private val random = new Random
  private val keysCount = scala.collection.mutable.Map[String, Int]()

  dataCollection.indexesManager.ensure(Index(background = true, unique = false, key = List(("key", IndexType.Ascending))))

  self ! LoadQueueSearch(loopMax)

  def receive = {
    case QueueSaveRequest(key, bytes) =>
      save(key, bytes)
    case QueueListenRequest(key) =>
      val lCount = listenerCount(key)
      keysCount(key) = lCount + 1
      val listener = sender
      val listenerId = manager.listen(key) { data =>
        listener ! DeQueueDataResponse(key, data)
      }
      listener ! QueueListenResponse(key, listenerId)
    case RemoveQueueListener(key, listenerId) =>
      val lCount = listenerCount(key)
      if (lCount == 1) {
        keysCount.remove(key)
      }
      else {
        keysCount(key) = lCount - 1
      }
      manager.removeListener(key, listenerId)
    case LoadQueueSearch(max) => loadSearch(max)
    case LoadQueueRequest(key, payload) => manager.save(key, payload)

  }

  private def keys = keysCount.filter { case (_, ct) => ct > 0 }.map { case (k, _) => k }.toList

  private def listenerCount(key: String) = keysCount.get(key).getOrElse(0)

  private def save(key: String, payload: Array[Byte]) {
    val doc = BSONDocument(
      "key" -> key,
      "payload" -> payload,
      "createdAt" -> System.currentTimeMillis,
      "tiebreaker" -> random.nextLong
    )
    dataCollection.insert(doc)
  }

  private def loadSearch(max: Int) {
    val myKeys = keys
    if(!myKeys.isEmpty && max > 0) {
      val selector = BSONDocument("key" -> BSONDocument("$in" -> myKeys))
      val sort = BSONDocument("createdAt" -> 1, "tiebreaker" -> -1)
      val remove = dataCollection.findAndRemove(selector, Some(sort))
      remove.onSuccess { case x =>
        x.value match {
          case Some(d) =>
            val key = d.get("key").get.asInstanceOf[BSONString].value
            val payload = d.get("payload").get.asInstanceOf[BSONBinary].byteArray
            if(keys.contains(key)) {
              self ! LoadQueueRequest(key, payload)
            }
            else {
              save(key, payload)
            }
            self ! LoadQueueSearch(max - 1)
          case _ =>
            in(500.milliseconds) {
              self ! LoadQueueSearch(loopMax)
            }
        }
      }

      remove.onFailure { case _ =>
        in(2.second) {
          self ! LoadQueueSearch(max - 1)
        }
      }
    }
    else {
      in(500.milliseconds) {
        self ! LoadQueueSearch(loopMax)
      }
    }
  }


  private def in(delay: FiniteDuration)(f: => Unit)  {
    Option(context).filter(!_.system.isTerminated).foreach { c =>
      c.system.scheduler.scheduleOnce(delay) {
        f
      }
    }
  }

}
