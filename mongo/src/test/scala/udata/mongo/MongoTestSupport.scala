package udata.mongo

import com.github.dockerjava.api.model.{Ports, ExposedPort}
import com.github.dockerjava.core.{DockerClientBuilder}

import java.net.ServerSocket

import org.scalatest.{BeforeAndAfterAll, FlatSpec}

import reactivemongo.api.{DefaultDB, MongoConnection, MongoDriver}


trait MongoSpec extends FlatSpec with BeforeAndAfterAll with MongoSupport {

  val driver = new MongoDriver
  import driver.system.dispatcher

  lazy val (host, port, close) = mongoServer
  Thread.sleep(2000)
  lazy val connection:MongoConnection = driver.connection(List(s"${host}:${port}"))
  lazy val database:DefaultDB = connection(java.util.UUID.randomUUID.toString.replace("-", "").substring(0, 5))

  override def afterAll(): Unit = {
    super.afterAll()
    if(connection != null) {
      connection.close()
    }
    driver.system.shutdown()
    close()
  }

}

trait MongoSupport {

  def mongoServer :(String, Int, () => Unit) = {
    val dockerClient = DockerClientBuilder.getInstance("unix:///var/run/docker.sock")
      .build()

    val socket = new ServerSocket(0)
    val openPort = socket.getLocalPort
    socket.close()
    val mongoPort = ExposedPort.tcp(27017)

    val host = "0.0.0.0"
    val portBindings = new Ports()
    portBindings.bind(mongoPort, Ports.Binding(host, openPort))

    val res = dockerClient.createContainerCmd("kperson/mongo")
      .withExposedPorts(mongoPort)
      .withPortBindings(portBindings)
      .exec()

    dockerClient.startContainerCmd(res.getId).exec()

    def close() {
      dockerClient.stopContainerCmd(res.getId).exec()
      dockerClient.waitContainerCmd(res.getId).exec(null)
      dockerClient.removeContainerCmd(res.getId).exec()
    }
    (host, openPort, close)
  }


  def withMongo(testCode: (String, Int) => Any): Unit = {
    val (host, port, close) = mongoServer
    testCode(host, port)
    close()
  }

}