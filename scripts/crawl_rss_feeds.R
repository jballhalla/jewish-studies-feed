library(httr)
library(jsonlite)
library(xml2)
library(lubridate)

# RSS feeds for Jewish news sources
rss_feeds <- data.frame(
  source = c("J. The Jewish News of Northern California", "eJewishPhilanthropy", 
             "Jewish Telegraphic Agency", "The Forward", "Times of Israel",
             "Jewish Journal", "Tablet Magazine", "Haaretz", "JNS", "Algemeiner"),
  url = c("https://www.jweekly.com/feed/",
          "https://ejewishphilanthropy.com/feed/",
          "https://www.jta.org/feed",
          "https://forward.com/feeds/all/",
          "https://www.timesofisrael.com/feed/",
          "https://jewishjournal.com/feed/",
          "https://www.tabletmag.com/rss",
          "https://www.haaretz.com/cmlink/1.628152",
          "https://www.jns.org/feed/",
          "https://www.algemeiner.com/feed/"),
  stringsAsFactors = FALSE
)

scrape_rss <- function(url, source_name) {
  tryCatch({
    response <- GET(url, timeout(30))
    if (status_code(response) != 200) {
      cat("Failed to fetch", source_name, "\n")
      return(NULL)
    }
    
    xml_content <- read_xml(content(response, "text", encoding = "UTF-8"))
    items <- xml_find_all(xml_content, "//item")
    
    articles <- data.frame(
      source = source_name,
      title = xml_text(xml_find_first(items, ".//title")),
      link = xml_text(xml_find_first(items, ".//link")),
      description = xml_text(xml_find_first(items, ".//description")),
      pub_date = xml_text(xml_find_first(items, ".//pubDate")),
      stringsAsFactors = FALSE
    )
    
    # Filter to last 24 hours
    articles$pub_date_parsed <- as.POSIXct(articles$pub_date, format = "%a, %d %b %Y %H:%M:%S", tz = "UTC")
    articles <- articles[!is.na(articles$pub_date_parsed), ]
    articles <- articles[articles$pub_date_parsed >= (Sys.time() - days(1)), ]
    
    return(articles)
  }, error = function(e) {
    cat("Error scraping", source_name, ":", e$message, "\n")
    return(NULL)
  })
}

# Scrape all RSS feeds
all_news <- list()
for (i in 1:nrow(rss_feeds)) {
  cat("Scraping", rss_feeds$source[i], "...\n")
  articles <- scrape_rss(rss_feeds$url[i], rss_feeds$source[i])
  if (!is.null(articles) && nrow(articles) > 0) {
    all_news[[i]] <- articles
  }
  Sys.sleep(1)  # Be polite
}

# Combine all news
if (length(all_news) > 0) {
  combined_news <- do.call(rbind, all_news)
  combined_news$date_scraped <- Sys.Date()
  
  # Create memory directory if it doesn't exist
  if (!dir.exists("./memory")) {
    dir.create("./memory")
  }
  
  # Load existing news data if it exists
  news_file <- "./memory/jewish_news.csv"
  if (file.exists(news_file)) {
    existing_news <- read.csv(news_file, stringsAsFactors = FALSE)
    # Remove articles older than 7 days
    existing_news$date_scraped <- as.Date(existing_news$date_scraped)
    existing_news <- existing_news[existing_news$date_scraped >= (Sys.Date() - 7), ]
    
    # Combine with new articles, remove duplicates
    all_articles <- rbind(existing_news, combined_news)
    all_articles <- all_articles[!duplicated(all_articles$link), ]
  } else {
    all_articles <- combined_news
  }
  
  # Save updated news data
  write.csv(all_articles, news_file, row.names = FALSE)
  cat("Scraped", nrow(combined_news), "new articles. Total stored:", nrow(all_articles), "\n")
} else {
  cat("No new articles found.\n")
}
