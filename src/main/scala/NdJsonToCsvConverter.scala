import java.security.MessageDigest

import akka.NotUsed
import akka.stream.{ Attributes, FlowShape, Inlet, Outlet }
import akka.stream.scaladsl.{ Sink, Source }
import akka.util.ByteString

import akka.stream.stage._

class NdJsonToCsvConverter extends GraphStage[FlowShape[ByteString, ByteString]] {
  val in = Inlet[ByteString]("DigestCalculator.in")
  val out = Outlet[ByteString]("DigestCalculator.out")

  override val shape = FlowShape(in, out)
  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = ???
}
