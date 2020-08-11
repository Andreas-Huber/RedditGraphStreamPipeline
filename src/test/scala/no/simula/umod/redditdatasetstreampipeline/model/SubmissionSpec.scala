package no.simula.umod.redditdatasetstreampipeline.model

import org.scalatest.flatspec.AnyFlatSpec

class SubmissionSpec extends AnyFlatSpec {

  "A submission" should "return all the provided fields given in the constructor" in {
    val submission = Submission(Option("Politics"), Option("555"))
    val result = submission.toCsvSeq

    assert(result(0) === "Politics")
    assert(result(1) === "555")
  }

  it should "return the same amount of headers as values" in {
    val submission = Submission(Option("Politics"), Option("555"))
    val fieldsLength = submission.toCsvSeq.length
    val headersLength = submission.getHeaders.length

    assert(fieldsLength === headersLength)
  }
}