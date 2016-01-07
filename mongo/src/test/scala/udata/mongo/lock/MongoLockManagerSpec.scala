package udata.mongo.lock

import akka.actor.{Props, ActorRef, ActorSystem}

import org.scalatest.Matchers
import org.scalatest.concurrent.ScalaFutures

import reactivemongo.api.DefaultDB

import udata.lock.LockManagerSpec
import udata.mongo.MongoSpec


class TestMongoLockManager(database: DefaultDB) extends MongoLockManager  {

  lazy val mongoCollection = database(java.util.UUID.randomUUID.toString.replace("-", "").substring(0, 10))

}

class MongoLockManagerSpec extends MongoSpec with ScalaFutures with Matchers with LockManagerSpec {

  lazy val displayName = "Mongo Lock Manager"

  def lockManager(system: ActorSystem): ActorRef = system.actorOf(Props(new TestMongoLockManager(database)))


}