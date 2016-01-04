package udata.aws.directory

import awscala._, s3._

import org.scalatest.BeforeAndAfterAll

import scala.concurrent.duration._
import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global

import udata.directory.{DirectoryListing, DirectorySpec}
import udata.util.TestUtils._


class TestS3Directory extends S3Directory {

  lazy val bucketName = java.util.UUID.randomUUID.toString.replace("-", "").substring(0, 10)
  lazy val s3 = S3()

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