package udata.mongo.lock

import reactivemongo.api.collections.bson.BSONCollection

import udata.mongo.MongoSupportConfig


class ConfigBasedMongoLockManager extends MongoLockManager {

  lazy val config = MongoSupportConfig.defaultConfig
  lazy val mongoCollection: BSONCollection = config.database(config.lockCollection)

}