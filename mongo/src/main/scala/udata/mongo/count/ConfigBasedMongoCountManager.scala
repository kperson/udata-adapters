package udata.mongo.count

import reactivemongo.api.collections.bson.BSONCollection

import udata.mongo.MongoSupportConfig


class ConfigBasedMongoCountManager() extends MongoCountManager {

  lazy val config = MongoSupportConfig.defaultConfig
  lazy val mongoCollection: BSONCollection = config.database(config.countCollection)

}