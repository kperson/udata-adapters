package udata.aws.directory

import awscala.s3._

import java.io._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{ExecutionContext, Future}

import udata.directory.{DirectoryListing, Directory}


object S3Directory {

  private [directory] case class S3Listing(resource: String, isFile: Boolean)

}

trait S3Directory extends Directory {

  import S3Directory._

  //dependencies
  def endpoint = "s3-external-1.amazonaws.com"

  def bucketName: String

  def s3: S3

  if (!s3.doesBucketExist(bucketName)) {
    s3.createBucket(bucketName)
  }
  val bucket = Bucket(bucketName)


  s3.setEndpoint(endpoint)

  def directories(path: List[String]): Future[List[String]] = {
    Future {
      val pathAsStr = if (path.isEmpty) "/" else path.mkString("/") + "/"
      scala.concurrent.blocking {
        s3.ls(bucket, pathAsStr).flatMap {
          case Left(prefix) =>
            val withTrailingSlash = prefix.replaceFirst(pathAsStr, "")
            Some(withTrailingSlash.substring(0, withTrailingSlash.length - 1))
          case _ => None
        }.toList
      }
    }
  }

  def files(path: List[String]): Future[List[String]] = {
    Future {
      scala.concurrent.blocking {
        val pathAsStr = if (path.isEmpty) "/" else path.mkString("/") + "/"
        s3.ls(bucket, pathAsStr).flatMap {
          case Right(summary) => Some(summary.getKey.replaceFirst(pathAsStr, ""))
          case z => None
        }.toList
      }
    }
  }

  def fileContents(path: List[String]): Future[Option[() => InputStream]] = {
    Future {
      scala.concurrent.blocking {
        s3.get(bucket, path.mkString("/")).map { a =>
          () => {
            a.content
          }
        }
      }
    }
  }

  def addFile(fileName: List[String]): OutputStream = {
    val localFile = File.createTempFile("tempfile", ".tmp")
    new CloseNoticeFileOutputStream(localFile.getAbsolutePath)({ file =>
      scala.concurrent.blocking {
        s3.put(bucket, fileName.mkString("/"), file)
        file.delete()
      }
    })
  }

  def delete(path: List[String]): Future[Unit] = {
    Future {
      scala.concurrent.blocking {
        s3.deleteObject(bucketName, path.mkString("/"))
      }
    }
  }

  override def directoryListing(path: List[String])(implicit ec: ExecutionContext): Future[DirectoryListing] = {
    Future {
      val pathAsStr = if (path.isEmpty) "/" else path.mkString("/") + "/"
      scala.concurrent.blocking {
        val listing = s3.ls(bucket, pathAsStr).map {
          case Left(prefix) =>
            val withTrailingSlash = prefix.replaceFirst(pathAsStr, "")
            S3Listing(withTrailingSlash.substring(0, withTrailingSlash.length - 1), false)
          case Right(summary) =>
            S3Listing(summary.getKey.replaceFirst(pathAsStr, ""), true)
        }
        val f = listing.filter(_.isFile == true).map {
          _.resource
        }
        val d = listing.filter(_.isFile == false).map {
          _.resource
        }
        DirectoryListing(f.toList, d.toList)
      }
    }
  }
}

class CloseNoticeFileOutputStream(fileName: String)(handler:(File) => Any) extends FileOutputStream(fileName) {

  override def close() {
    super.close()
    handler(new File(fileName))

  }

}

