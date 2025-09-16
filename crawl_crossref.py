#!/usr/bin/env python3
"""
Crossref Academic Crawler for Jewish Studies Feed
Crawls Crossref API weekly for new academic articles
"""

import os
import sys
from datetime import datetime
from src.crossref_crawler import CrossrefCrawler

def main():
    """Main function to crawl Crossref API"""
    print(f"Starting Crossref crawl at {datetime.now()}")
    
    # Initialize crawler
    crawler = CrossrefCrawler(
        journals_file='config/jewish_studies_journals.csv',
        memory_file='data/memory/research_log.csv'
    )
    
    # Crawl journals (look back 8 days to ensure we don't miss anything with weekly runs)
    articles = crawler.crawl_all_journals(days_back=8)
    
    # Save articles to memory
    crawler.save_articles(articles)
    
    # Generate output JSON (last 7 days)
    crawler.generate_output_json('data/output/research_articles.json', days_back=7)
    
    print(f"Crossref crawl completed. Found {len(articles)} new articles.")
    
    if articles:
        print("\nSample of new articles:")
        for i, article in enumerate(articles[:3]):
            print(f"{i+1}. {article['title']} - {article['authors']} ({article['journal_name']})")

if __name__ == "__main__":
    main()
