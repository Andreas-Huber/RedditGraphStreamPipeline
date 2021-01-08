package no.simula.umod.redditdatasetstreampipeline.model

trait StreamMarker {
  def isEndOfStream: Boolean
}
