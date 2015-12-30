package udata.mongo.count

import akka.actor.{Props, ActorRef, ActorSystem}

import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{Matchers, FlatSpec}

import reactivemongo.api.DefaultDB

import udata.count.{LocalCountManager, CountManagerSpec}
import udata.mongo.MongoSpec


class TestMongoCountManger(database: DefaultDB) extends MongoCountManager  {

  lazy val mongoCollection = database(java.util.UUID.randomUUID.toString.replace("-", "").substring(0, 10))

}

class MongoCountManagerSpec extends MongoSpec with CountManagerSpec {

  def countManager(system: ActorSystem): ActorRef = system.actorOf(Props(new TestMongoCountManger(database)))

}