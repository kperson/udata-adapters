package udata.mongo.count

import reactivemongo.api.collections.bson.BSONCollection

import udata.mongo.MongoSupportConfig


class ConfigurableMongoCountManager extends MongoCountManager {

  val config = MongoSupportConfig.defaultConfig
  def mongoCollection: BSONCollection = config.database(config.countCollection)

}