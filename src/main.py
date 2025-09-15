#!/usr/bin/env python3

import sys
import os
from datetime import datetime
import json
import pandas as pd
from src.rss_crawler import RSSCrawler
from src.crossref_crawler import CrossrefCrawler

def main():
    if len(sys.argv) != 2:
        print("Usage: python main.py [news|research]")
        sys.exit(1)
    
    crawl_type = sys.argv[1]
    
    if crawl_type == "news":
        crawl_news()
    elif crawl_type == "research":
        crawl_research()
    else:
        print("Invalid crawl type. Use 'news' or 'research'")
        sys.exit(1)

def crawl_news():
    """Crawl RSS feeds for news"""
    print("Crawling news feeds...")
    
    crawler = RSSCrawler(
        feeds_file='config/jewish_news_feeds.csv',
        memory_file='data/memory/news_log.csv'
    )
    
    articles = crawler.crawl_all_feeds()
    crawler.save_articles(articles)
    
    # Generate output JSON (last 7 days)
    generate_news_json()
    
    print(f"Found {len(articles)} new articles")

def crawl_research():
    """Crawl Crossref for research articles"""
    print("Crawling research articles...")
    
    email = os.getenv('CROSSREF_EMAIL', 'your-email@domain.com')
    
    crawler = CrossrefCrawler(
        journals_file='config/jewish_studies_journals.csv',
        email=email
    )
    
    articles = crawler.crawl_journals(days_back=14)
    save_research_articles(articles)
    
    # Generate output JSON
    generate_research_json(articles)
    
    print(f"Found {len(articles)} new articles")

def save_research_articles(articles):
    """Save research articles to memory"""
    if not articles:
        return
    
    df = pd.DataFrame(articles)
    memory_file = 'data/memory/research_log.csv'
    
    try:
        existing_df = pd.read_csv(memory_file)
        combined_df = pd.concat([existing_df, df], ignore_index=True)
    except FileNotFoundError:
        combined_df = df
    
    combined_df = combined_df.drop_duplicates(subset=['url'])
    combined_df.to_csv(memory_file, index=False)

def generate_news_json():
    """Generate news output JSON"""
    try:
        df = pd.read_csv('data/memory/news_log.csv')
        df['scraped_at'] = pd.to_datetime(df['scraped_at'])
        
        # Last 7 days only
        cutoff = datetime.now() - pd.Timedelta(days=7)
        recent_df = df[df['scraped_at'] >= cutoff]
        
        output = {
            'update': datetime.now().isoformat(),
            'content': recent_df.to_dict('records')
        }
        
        with open('data/output/news_articles.json', 'w') as f:
            json.dump(output, f, indent=2, ensure_ascii=False)
            
    except FileNotFoundError:
        # Create empty output
        output = {
            'update': datetime.now().isoformat(),
            'content': []
        }
        
        with open('data/output/news_articles.json', 'w') as f:
            json.dump(output, f, indent=2)

def generate_research_json(articles):
    """Generate research output JSON"""
    output = {
        'update': datetime.now().isoformat(),
        'content': articles
    }
    
    with open('data/output/research_articles.json', 'w') as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

if __name__ == "__main__":
    main()
