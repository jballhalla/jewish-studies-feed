#!/usr/bin/env python3
"""
RSS News Crawler for Jewish Studies Feed
Crawls RSS feeds hourly and saves to memory + generates output JSON
"""

import os
import sys
from datetime import datetime
from src.rss_crawler import RSSCrawler

def main():
    """Main function to crawl RSS feeds"""
    print(f"Starting RSS crawl at {datetime.now()}")
    
    # Initialize crawler
    crawler = RSSCrawler(
        feeds_file='config/jewish_news_feeds.csv',
        memory_file='data/memory/news_log.csv'
    )
    
    # Crawl feeds (look back 25 hours to ensure we don't miss anything with hourly runs)
    articles = crawler.crawl_all_feeds(hours_back=25)
    
    # Save articles to memory
    crawler.save_articles(articles)
    
    # Generate output JSON (last 7 days)
    crawler.generate_output_json('data/output/news_articles.json', days_back=7)
    
    print(f"RSS crawl completed. Found {len(articles)} new articles.")
    
    if articles:
        print("\nSample of new articles:")
        for i, article in enumerate(articles[:3]):
            print(f"{i+1}. {article['title']} ({article['source']})")

if __name__ == "__main__":
    main()
