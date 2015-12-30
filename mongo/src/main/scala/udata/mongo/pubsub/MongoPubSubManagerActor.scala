package udata.mongo.pubsub

import akka.actor.Actor

import reactivemongo.api.collections.bson.BSONCollection
import reactivemongo.api.commands.GetLastError
import reactivemongo.api.indexes.{IndexType, Index}
import reactivemongo.bson._

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.util.Random

import udata.pubsub.{PubSubManager, PubSubManagerActor}


object MongoPubSubManagerActor {

  case class LoadRequest(key: String, bytes: Array[Byte])
  case object LoadSearch
  case object MarkConnected
  case object Clean

  val pingFrequency = 10.seconds
  val localTimeout = 25.seconds
  val mongoTimeout = 40.seconds
  val consumeTimeout = 30.minutes

}

trait MongoPubSubManagerActor extends Actor {

  //dependencies
  def connectionCollection: BSONCollection
  def dataCollection: BSONCollection
  def manager:PubSubManager[Array[Byte]]

  import PubSubManagerActor._
  import MongoPubSubManagerActor._
  import context.dispatcher

  private val random = new Random

  private var lastConnect = System.currentTimeMillis
  private var serverId = java.util.UUID.randomUUID.toString


  connectionCollection.indexesManager.ensure(Index(background = true, unique = false, key = List(("key", IndexType.Ascending))))
  dataCollection.indexesManager.ensure(Index(background = true, unique = false, key = List(("key", IndexType.Ascending))))

  self ! MarkConnected
  self ! LoadSearch
  self ! Clean

  def receive = {
    case AddListenerRequest(key) =>
      val listener = sender
      val hasListenersAlready:Boolean = manager.keys.contains(key)
      val actorId = manager.addListener(key) { x =>
        listener ! PushedData(x.dataId, x.payload)
      }
      if(!hasListenersAlready && isConnected) {
        registerKeys(List(key)).onComplete { case _ =>
          listener ! AddListenerResponse(key, actorId)
        }
      }
      else {
        listener ! AddListenerResponse(key, actorId)
      }
    case SaveRequest(key, bytes) => save(key, bytes)
    case LoadRequest(key, bytes) => manager.save(key, bytes)

    case ReceivedAckRequest(key, dataId, listenerId) =>
      manager.waitForNext(key, dataId, listenerId)
    case RemoveListenerRequest(key, listenerId) =>
      manager.removeListener(key, listenerId)
      if (!manager.keys.contains(key)) {
        val selector = BSONDocument("key" -> key, "listenerId" -> serverId)
        val connectionRemoval = connectionCollection.remove(selector, GetLastError.Acknowledged)
        Await.result(connectionRemoval, 5.seconds)
      }
    case LoadSearch => loadSearch()
    case MarkConnected => markConnection()
    case Clean =>
      if(isConnected) {
        connectionCollection.remove(BSONDocument("lastConnect" -> BSONDocument("$lt" -> (System.currentTimeMillis - consumeTimeout.toMillis))), firstMatchOnly = false)
        dataCollection.remove(BSONDocument("createdAt" -> BSONDocument("$lt" -> (System.currentTimeMillis - consumeTimeout.toMillis))), firstMatchOnly = false)
      }
      in(5.minutes) {
        self ! Clean
      }
  }


  def save(key: String, payload: Array[Byte]) {
    val transactionId = java.util.UUID.randomUUID.toString
    val listenersSelector = BSONDocument("key" -> key, "lastConnect" -> BSONDocument("$gt" -> (System.currentTimeMillis - mongoTimeout.toMillis)))
    connectionCollection.find(listenersSelector, BSONDocument("listenerId" -> 1)).cursor[BSONDocument].collect[List]().onSuccess { case retains =>
      var rts = BSONArray()
      retains.foreach { x =>
        rts = rts.add(BSONDocument("listenerId" -> x.get("listenerId").get.asInstanceOf[BSONString].value))
      }
      if (!retains.isEmpty) {
        val doc = BSONDocument(
          "payload" -> payload,
          "retains" -> rts,
          "retainCt" -> retains.size,
          "key" -> key,
          "transactionId" -> transactionId,
          "createdAt" -> System.currentTimeMillis,
          "tiebreaker" -> random.nextLong
        )
        dataCollection.insert(doc, GetLastError.Unacknowledged)
      }
    }
  }


  private def loadSearch() {
    val listenerId = serverId
    if(!isConnected) {
      serverId = java.util.UUID.randomUUID.toString
      val now  = System.currentTimeMillis
      val registration = registerKeys(manager.keys, now)
      registration.onSuccess { case _ =>
        lastConnect = now
      }
      registration.onComplete { case _ =>
        in(1.seconds) {
          //allow a bit of work to be done before retrying, let the database recover a bit
          self ! LoadSearch
          self ! MarkConnected
        }
      }
    }
    else {
      if (!manager.keys.isEmpty) {
        val selector = BSONDocument("key" -> BSONDocument("$in" -> manager.keys), "retains" -> BSONDocument("$elemMatch" -> BSONDocument("listenerId" -> listenerId)))
        val project = BSONDocument("payload" -> 1, "key" -> 1, "transactionId" -> 1)
        val newItems = dataCollection.find(selector, project).sort(BSONDocument("createdAt" -> 1, "tiebreaker" -> -1)).cursor[BSONDocument].collect[List]()
        val loadData = newItems.flatMap { x =>
          val data = x.map { d =>
            val key = d.get("key").get.asInstanceOf[BSONString].value
            val transactionId = d.get("transactionId").get.asInstanceOf[BSONString].value
            val payload = d.get("payload").get.asInstanceOf[BSONBinary].byteArray
            decrement(key, transactionId, listenerId).map { case _ =>
              self ! LoadRequest(key, payload)
            }
          }
          Future.sequence(data)
        }
        loadData.onComplete { case _ =>
          scheduleSearch()
        }
      }
      else {
        scheduleSearch()
      }
    }
  }

  private def scheduleSearch() {
    in(400.milliseconds) {
      self ! LoadSearch
    }
  }

  private def registerKeys(myKeys: List[String], myLastConnect: Long = lastConnect) : Future[Any] = {
    val registration = myKeys.map { k =>
      connectionCollection.insert(BSONDocument("listenerId" -> serverId, "lastConnect" -> myLastConnect, "key" -> k), GetLastError.Unacknowledged)
    }
    Future.sequence(registration)
  }


  private def decrement(key: String, transactionId: String, listenerId: String = serverId) : Future[Any] = {
    val selector = BSONDocument("key" -> key, "transactionId" -> transactionId, "retains" -> BSONDocument("$elemMatch" -> BSONDocument("listenerId" -> listenerId)))
    val update = BSONDocument("$inc" -> BSONDocument("retainCt" -> -1), "$pull" -> BSONDocument(s"retains" -> BSONDocument("listenerId" -> listenerId)))
    dataCollection.findAndUpdate(selector, update, fetchNewObject = true, upsert = false).flatMap { x =>
      x.value.filter(_.get("retainCt").get.asInstanceOf[BSONInteger].value == 0).map { q =>
        dataCollection.remove(BSONDocument(key -> key, "transactionId" -> transactionId), writeConcern = GetLastError.Acknowledged)
      }.getOrElse(Future.successful(Unit))
    }
  }

  private def markConnection() {
    if(isConnected) {
      if(manager.keys.isEmpty) {
        //we only need to store our connection time locally if we are connecting to the database
        lastConnect = System.currentTimeMillis
        in(1.seconds) {
          self ! MarkConnected
        }
      }
      else {
        val now = System.currentTimeMillis
        val selector = BSONDocument("listenerId" -> serverId, "lastConnect" -> BSONDocument("$gt" -> (now - localTimeout.toMinutes)))
        val update = BSONDocument("lastConnect" -> now)
        val connectionUpdate = connectionCollection.update(selector, update, multi = true, upsert = false)
        connectionUpdate.onSuccess { case _ =>
          lastConnect = now
        }
        connectionUpdate.onComplete { case _ =>
          in(pingFrequency) {
            self ! MarkConnected
          }
        }
      }
    }
  }

  private def isConnected = lastConnect > System.currentTimeMillis - MongoPubSubManagerActor.localTimeout.toMillis

  private def in(delay: FiniteDuration)(f: => Unit)  {
    Option(context).filter(!_.system.isTerminated).foreach { c =>
      c.system.scheduler.scheduleOnce(delay) {
        f
      }
    }
  }

}