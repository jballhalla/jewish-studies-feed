field <- "jewish_studies"
library(httr)
library(jsonlite)

source("fun.R")
source("credentials.R")
source("./parameters/prompts.R")

now <- Sys.time()
crawl_start_date <- as.Date(now) - 14
crawl_end_date <- as.Date(now) - 1

journals <- read.csv(paste0("./parameters/", field, "_journals.csv"))
past_urls <- read.csv(paste0("./memory/", field, "_urls.csv"))

use_polite <- crossref_endpoint_polite_faster(crawl_start_date,crawl_end_date)

# Crawl Crossref API 
out <- retrieve_crossref_issn_data(
    issn_list=journals$issn, 
    start_date=crawl_start_date, 
    end_date=crawl_end_date, 
    verbose=TRUE, polite_endpoint=use_polite)

# Remove duplicates
out <- out[!duplicated(out$url),] 
# Remove past paers
out <- out[!(out$url %in% past_urls$url), ]
if(is.null(out)) {
    json <- toJSON(list("update"=as.Date(now), "content"=list()), 
        pretty=TRUE, auto_unbox=TRUE)
    write(json, paste0("./output/", field, ".json"))
    quit(save="no")
    } 

# Cleanup data
out$abstract <- strip_html(out$abstract)
out$abstract <- gsub("^(Abstract|ABSTRACT) ", "", out$abstract)
out$title <- strip_html(out$title)
out$doi <- extract_doi_id(out$url)

# Merge in journal information 
out <- merge(out, journals, by="issn")

# Apply standard filter flags 
out <- add_standard_filter(out) 

# Filter flags: Multidisciplinary journals 
if(field %in% c("multidisciplinary", "environmental_and_climate_politics_studies") ){
    out_lst <- split(out, out$filter_function) 
    out_lst <- lapply(out_lst, dispatch_special_filter ) 
    out <- do.call(rbind, out_lst)
    out$filter <- apply(out, 1, function(x){
        tryCatch(
            {add_multidisciplinary_filter(x)
            }, error = function(msg){
                return(-1)
            })
        })
    rownames(out) <- NULL
    } 

# Load news articles and combine with academic articles
news_file <- "./memory/jewish_news.csv"
combined_articles <- list()

if (file.exists(news_file)) {
  news_data <- read.csv(news_file, stringsAsFactors = FALSE)
  news_data$date_scraped <- as.Date(news_data$date_scraped)
  
  # Only include articles from the last 7 days
  recent_news <- news_data[news_data$date_scraped >= (Sys.Date() - 7), ]
  
  if (nrow(recent_news) > 0) {
    # Prepare news articles for AI ranking
    news_for_ai <- data.frame(
      type = "news",
      title = recent_news$title,
      description = recent_news$description,
      url = recent_news$link,
      source = recent_news$source,
      stringsAsFactors = FALSE
    )
    
    combined_articles[["news"]] <- news_for_ai
  }
}

# Prepare academic articles for AI ranking
if (nrow(out) > 0) {
  academic_for_ai <- data.frame(
    type = "academic",
    title = out$title,
    description = ifelse(is.na(out$abstract) | out$abstract == "", 
                        paste("Academic article from", out$journal_full), 
                        out$abstract),
    url = out$url,
    source = out$journal_full,
    stringsAsFactors = FALSE
  )
  
  combined_articles[["academic"]] <- academic_for_ai
}

# If we have articles, use OpenAI to rank them
if (length(combined_articles) > 0) {
  all_for_ranking <- do.call(rbind, combined_articles)
  
  if (nrow(all_for_ranking) > 0) {
    source("./functions/openai_ranking.R")
    ranked_articles <- rank_articles_with_openai(all_for_ranking)
    
    # Filter original academic articles based on OpenAI ranking
    if (!is.null(ranked_articles)) {
      top_academic_urls <- ranked_articles[ranked_articles$type == "academic", "url"]
      out <- out[out$url %in% top_academic_urls, ]
    }
  }
}

# Output JSON
out_json <- render_json(out, date=as.Date(now)) 
write(out_json, paste0("./output/", field, ".json"))
write(out_json, paste0("./docs/output/", field, ".json"))

# Update past urls
write.table(out[,"url"], 
    file=paste0("./memory/", field, "_urls.csv"), 
    na="", 
    sep=";", 
    append=TRUE, 
    quote=FALSE, 
    col.names=FALSE,
    row.names=FALSE)

# Write journal short list
journals_out <- unique(journals[,c("journal_full","journal_short")])
journals_out <- journals_out[order(journals_out$journal_full),]
journals_out <- toJSON(journals_out, pretty=TRUE, auto_unbox=TRUE) 
write(journals_out, paste0("./output/", field, "_journals.json"))
