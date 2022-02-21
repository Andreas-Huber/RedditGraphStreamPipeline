package no.simula.umod.redditdatasetstreampipeline.model

import scala.collection.immutable

trait ToCsv {
  def toCsvSeq: immutable.Seq[String]

  def getHeaders: immutable.Seq[String]
}
