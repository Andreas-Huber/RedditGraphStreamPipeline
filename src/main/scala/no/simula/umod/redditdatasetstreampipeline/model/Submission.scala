package no.simula.umod.redditdatasetstreampipeline.model

case class Submission(
 subreddit: Option[String],
 id: Option[String],
 author: Option[String],
 title: Option[String]
) extends ToCsv {

  override def toCsvSeq: Seq[String] =  Seq(
    subreddit.getOrElse(""),
    id.getOrElse(""),
    author.getOrElse(""),
    title.getOrElse("")
  )

  override def getHeaders: Seq[String] = Seq(
    "subreddit",
    "id",
    "author",
    "title"
  )
}

/*
Sample Submission JSON:

{
    "archived":true,
    "author":"[deleted]",
    "author_flair_background_color":"",
    "author_flair_css_class":null,
    "author_flair_text":null,
    "author_flair_text_color":"dark",
    "brand_safe":true,
    "can_gild":false,
    "contest_mode":false,
    "created_utc":1230768001,
    "distinguished":null,
    "domain":"unemployedguide.blogspot.com",
    "edited":false,
    "gilded":0,
    "hidden":false,
    "hide_score":false,
    "id":"7mq3w",
    "is_crosspostable":false,
    "is_reddit_media_domain":false,
    "is_self":false,
    "is_video":false,
    "link_flair_css_class":null,
    "link_flair_richtext":[

    ],
    "link_flair_text":null,
    "link_flair_text_color":"dark",
    "link_flair_type":"text",
    "locked":false,
    "media":null,
    "media_embed":{

    },
    "no_follow":true,
    "num_comments":0,
    "num_crossposts":0,
    "over_18":false,
    "parent_whitelist_status":"all_ads",
    "permalink":"\/r\/offbeat\/comments\/7mq3w\/laura_ingrahams_embryo\/",
    "retrieved_on":1522770231,
    "rte_mode":"markdown",
    "score":1,
    "secure_media":null,
    "secure_media_embed":{

    },
    "selftext":"[deleted]",
    "send_replies":true,
    "spoiler":false,
    "stickied":false,
    "subreddit":"offbeat",
    "subreddit_id":"t5_2qh11",
    "subreddit_name_prefixed":"r\/offbeat",
    "subreddit_type":"public",
    "suggested_sort":null,
    "thumbnail":"default",
    "thumbnail_height":null,
    "thumbnail_width":null,
    "title":"Laura Ingraham's Embryo...",
    "url":"http:\/\/unemployedguide.blogspot.com\/2008\/12\/laura-ingrahams-embryo.html",
    "whitelist_status":"all_ads"
}
 */