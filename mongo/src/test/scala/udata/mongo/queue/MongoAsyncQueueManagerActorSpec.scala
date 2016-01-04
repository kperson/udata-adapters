package udata.mongo.queue

import akka.actor.{Props, ActorSystem, ActorRef}

import reactivemongo.api.DefaultDB

import udata.mongo.MongoSpec
import udata.queue.{AsyncQueueManager, AsyncQueueManagerActorSpec}


class TestMongoQueueManager(val manager: AsyncQueueManager[Array[Byte]], database: DefaultDB) extends MongoAsyncQueueManagerActor {

  lazy val dataCollection = database(java.util.UUID.randomUUID.toString.replace("-", "").substring(0, 10))

}

class MongoAsyncQueueManagerActorSpec extends AsyncQueueManagerActorSpec with MongoSpec {

  def displayName = "Mongo Async Queue Manager Actor"

  def withManager(manager: AsyncQueueManager[ByteArray])(testCode: (ActorRef) => Unit) {
    val actorRef = system.actorOf(Props(new TestMongoQueueManager(manager, database)))
    testCode(actorRef)
    system.stop(actorRef)
  }

  def system: ActorSystem = driver.system

}