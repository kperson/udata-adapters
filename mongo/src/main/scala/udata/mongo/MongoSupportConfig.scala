package udata.mongo

import com.typesafe.config.{ConfigFactory, Config}
import reactivemongo.api.{DefaultDB, MongoDriver, MongoConnection}


class MongoSupportConfig {

  val config: Config = ConfigFactory.load().getConfig("udata-hub.mongo")

  private lazy val driver = new MongoDriver
  import driver.system.dispatcher

  private lazy val databaseURI = config.getString("uri")
  private lazy val databaseName = config.getString("db")

  lazy val connection: MongoConnection = driver.connection(MongoConnection.parseURI(databaseURI).get)
  lazy val database: DefaultDB = connection(databaseName)

  lazy val collectionPrefix = config.getString("prefix")

  lazy val countCollection = s"${collectionPrefix}_count"
  lazy val lockCollection = s"${collectionPrefix}_lock"
  lazy val pubSubCollection = s"${collectionPrefix}_pubsub"
  lazy val pubSubConnectionCollection = s"${collectionPrefix}_pubsub_connection"

  lazy val queueCollection = s"${collectionPrefix}_queue"
  lazy val directoryCollection = s"${collectionPrefix}_queue"

}

object MongoSupportConfig {

  lazy val defaultConfig = new MongoSupportConfig()

}
