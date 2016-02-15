package udata.aws.directory

import awscala._
import awscala.s3.S3
import com.amazonaws.ClientConfiguration
import com.typesafe.config.{ConfigFactory, Config}


class ConfigBasedS3Directory extends S3Directory {

  lazy val s3Config = new ClientConfiguration()
    .withMaxErrorRetry (15)
    .withConnectionTimeout(120 * 1000)
    .withSocketTimeout (120 * 1000)
    .withTcpKeepAlive (true)
  lazy val config: Config = ConfigFactory.load().getConfig("udata-hub.aws")
  lazy val s3 = new S3ConfigClient(config.getString("aws-key"), config.getString("aws-secret"), s3Config).at(Region.default)
  if (config.hasPath("s3-endpoint")) {
    s3.setEndpoint(config.getString("s3-endpoint"))
  }
  lazy val bucketName = config.getString("directory-bucket")

}