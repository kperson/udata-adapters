package udata.aws.directory

import awscala._
import awscala.s3.S3
import com.typesafe.config.{ConfigFactory, Config}


class ConfigBasedS3Directory extends S3Directory {

  lazy val config: Config = ConfigFactory.load().getConfig("udata-hub.aws")
  lazy val s3 = S3(config.getString("aws-key"), config.getString("aws-secret"))(Region.default())
  lazy val bucketName = config.getString("directory-bucket")

}