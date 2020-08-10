package no.simula.umod.redditdatasetstreampipeline

import java.nio.file.Path

import akka.stream.scaladsl.{Broadcast, FileIO, Flow, GraphDSL, Sink, Source, ZipWith}
import akka.stream.{IOResult, Materializer, SinkShape}
import akka.util.ByteString

import scala.concurrent.Future

object FileSinks {
  def dispatch[T](
                   dispatcher: T => Path,
                   serializer: T => ByteString
                 )(
                   implicit materializer: Materializer
                 ): Sink[T, Future[Seq[Future[IOResult]]]] =
    Sink.fromGraph(
      GraphDSL.create(
        Sink.seq[Future[IOResult]]
      ) {
        implicit builder =>
          sink =>
            // prepare this sink's graph elements:
            val broadcast = builder.add(Broadcast[T](2))
            val serialize = builder.add(Flow[T].map(serializer))
            val dispatch = builder.add(Flow[T].map(dispatcher))
            val zipAndWrite = builder.add(ZipWith[ByteString, Path, Future[IOResult]](
              (bytes, path) => Source.single(bytes).runWith(FileIO.toPath(path)))
            )

            // connect the graph:
            import GraphDSL.Implicits._
            broadcast.out(0) ~> serialize ~> zipAndWrite.in0
            broadcast.out(1) ~> dispatch ~> zipAndWrite.in1
            zipAndWrite.out ~> sink

            // expose ports:
            SinkShape(broadcast.in)
      }
    )
}
