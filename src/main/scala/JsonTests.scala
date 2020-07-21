//import spray.json._
//import DefaultJsonProtocol._
//import model.Submission
//
//object JsonTests extends App{
//
//  // create the formats and provide them implicitly
//  implicit val submissionFormat = jsonFormat2(Submission)
//
//
//  val source = """{ "subreddit": "MySubreddit", "id":"ab123"}"""
//  val jsonAst = source.parseJson // or JsonParser(source)
//  val submission = jsonAst.convertTo[Submission]
//
//  Console.println(submission.subreddit)
//}
