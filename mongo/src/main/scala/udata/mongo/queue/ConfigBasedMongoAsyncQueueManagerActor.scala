package udata.mongo.queue

import reactivemongo.api.collections.bson.BSONCollection

import udata.mongo.MongoSupportConfig
import udata.queue.AsyncQueueManager


class ConfigBasedMongoAsyncQueueManagerActor extends MongoAsyncQueueManagerActor {

  lazy val config = MongoSupportConfig.defaultConfig
  lazy val dataCollection: BSONCollection = config.database(config.queueCollection)
  lazy val manager:AsyncQueueManager[Array[Byte]] = new AsyncQueueManager[Array[Byte]]()

}