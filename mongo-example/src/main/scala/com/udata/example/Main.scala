package com.udata.example

import akka.actor.{Actor, Props}
import akka.io.IO

import spray.can.Http

import udata.directory.Directory
import udata.hub.HubServer
import udata.{HubServerConfig, HubActorSystem}


object Main extends App {

  case class ServerArguments(host: String = "0.0.0.0", port: Int = 8080)

  def parser() = new scopt.OptionParser[ServerArguments]("hub") {

    head("udata", "0.1")

    opt[String]("host") action { (x, c) =>
      c.copy(host = x)
    } text ("the host the server will bind to")

    opt[Int]("port") action { (x, c) =>
      c.copy(port = x)
    } text ("the port the server will bind to")

  }

  parser().parse(args, ServerArguments()) match {
    case Some(config) =>
      implicit val system = HubActorSystem.system

      val serverConfig = new HubServerConfig()

      val directory = Class.forName(serverConfig.directoryManagerClassName).newInstance().asInstanceOf[Directory]
      val lockProps = Props(Class.forName(serverConfig.lockManagerClassName).asInstanceOf[Class[Actor]])
      val pubSubProps = Props(Class.forName(serverConfig.pubSubManagerClassName).asInstanceOf[Class[Actor]])
      val queueProps = Props(Class.forName(serverConfig.queueManagerClassName).asInstanceOf[Class[Actor]])
      val countProps = Props(Class.forName(serverConfig.countManagerClassName).asInstanceOf[Class[Actor]])

      val handler = system.actorOf(Props(
        new HubServer(
          directory,
          lockProps,
          pubSubProps,
          queueProps,
          countProps
        )).withDispatcher("akka.pubsub-dispatcher"))
      IO(Http) ! Http.Bind(handler, interface = config.host, port = config.port)
    case _ => println("--help for details")
  }

}