package no.simula.umod.redditdatasetstreampipeline

import java.io.File

// todo: https://github.com/scopt/scopt
// todo: https://stackoverflow.com/questions/2315912/best-way-to-parse-command-line-parameters
case class Config (
  submissions: Boolean = false,
  comments: Boolean = false,
  submissionsFile: File = new File("submissionsstream"),
  commentsFile: File = new File("commentsstream"),
)

