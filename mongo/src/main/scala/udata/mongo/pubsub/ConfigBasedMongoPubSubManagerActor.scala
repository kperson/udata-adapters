package udata.mongo.pubsub

import reactivemongo.api.collections.bson.BSONCollection

import udata.mongo.MongoSupportConfig
import udata.pubsub.{LocalPubSubManager, PubSubManager}


class ConfigBasedMongoPubSubManagerActor() extends MongoPubSubManagerActor {

  lazy val config = MongoSupportConfig.defaultConfig
  lazy val connectionCollection: BSONCollection = config.database(config.pubSubConnectionCollection)
  lazy val dataCollection: BSONCollection = config.database(config.pubSubCollection)
  lazy val manager:PubSubManager[Array[Byte]] = new LocalPubSubManager[Array[Byte]]()

}