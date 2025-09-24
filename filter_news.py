#!/usr/bin/env python3
"""
News Filter for Jewish Studies Feed
Filters weekly news articles using Anthropic API for research relevance
"""

import os
import sys
from datetime import datetime
from src.news_filter import NewsFilter

def main():
    """Main function to filter weekly news"""
    print(f"Starting news filtering at {datetime.now()}")
    
    # Check for API key
    if not os.getenv('ANTHROPIC_API_KEY'):
        print("ERROR: ANTHROPIC_API_KEY environment variable is required")
        sys.exit(1)
    
    # Initialize filter
    news_filter = NewsFilter(
        memory_file='data/memory/news_log.csv',
        output_file='data/output/news_articles.json'
    )
    
    try:
        # Filter articles from the past week
        filtered_articles, total_processed = news_filter.filter_weekly_news(days_back=7)
        
        print(f"News filtering completed.")
        print(f"Processed: {total_processed} articles")
        print(f"Filtered: {len(filtered_articles)} research-relevant articles")
        
        if filtered_articles:
            print("\nSample of filtered articles:")
            for i, article in enumerate(filtered_articles[:3]):
                print(f"{i+1}. {article['title']} ({article['source']})")
        else:
            print("\nNo research-relevant articles found this week.")
            
    except Exception as e:
        print(f"ERROR: News filtering failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
