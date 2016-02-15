package udata.aws.directory

import awscala._, s3._
import com.amazonaws.ClientConfiguration
import org.apache.commons.io.IOUtils

import org.scalatest.BeforeAndAfterAll

import scala.collection.JavaConverters._

import udata.directory.DirectorySpec


class TestS3Directory extends S3Directory {

  lazy val bucketName = java.util.UUID.randomUUID.toString.replace("-", "").substring(0, 10)
  lazy val s3Config = new ClientConfiguration()
    .withMaxErrorRetry (15)
    .withConnectionTimeout(120 * 1000)
    .withSocketTimeout (120 * 1000)
    .withTcpKeepAlive (true)
  lazy val s3 = new S3ConfigClient(config = s3Config)

  def clean() {
    s3.listObjects(bucketName).getObjectSummaries.asScala.foreach { summary =>
      s3.deleteObject(summary.getBucketName, summary.getKey)
    }
    s3.delete(Bucket(bucketName))
  }

}

class S3DirectorySpec extends DirectorySpec with BeforeAndAfterAll {

  behavior of "S3 Directory"

  lazy val directory = new TestS3Directory()

  override def afterAll() {
    super.afterAll()
    directory.clean()
  }

}