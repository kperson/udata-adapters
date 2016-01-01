package udata.mongo.pubsub

import akka.actor.{Props, ActorRef}

import reactivemongo.api.DefaultDB

import udata.mongo.MongoSpec
import udata.pubsub.{PubSubManager, PubSubManagerActorSpec}


class TestMongoPubSubManager(val manager: PubSubManager[Array[Byte]], database: DefaultDB) extends MongoPubSubManagerActor {

  lazy val connectionCollection = database(java.util.UUID.randomUUID.toString.replace("-", "").substring(0, 10))
  lazy val dataCollection = database(java.util.UUID.randomUUID.toString.replace("-", "").substring(0, 10))

}

class MongoPubSubManagerActorSpec extends PubSubManagerActorSpec with MongoSpec {

  def system = driver.system

  def withManager(manager: PubSubManager[Array[Byte]])(testCode: (ActorRef) => Unit) {
    val actorRef = system.actorOf(Props(new TestMongoPubSubManager(manager, database)))
    testCode(actorRef)
    system.stop(actorRef)
  }

}