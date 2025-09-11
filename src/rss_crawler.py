import feedparser
import pandas as pd
import requests
from datetime import datetime, timedelta
import logging
from typing import List, Dict, Optional
import time

class RSSCrawler:
    def __init__(self, feeds_file: str, memory_file: str):
        self.feeds_df = pd.read_csv(feeds_file)
        self.memory_file = memory_file
        self.logger = self._setup_logger()
    
    def _setup_logger(self):
        logging.basicConfig(level=logging.INFO)
        return logging.getLogger(__name__)
    
    def crawl_all_feeds(self) -> List[Dict]:
        """Crawl all RSS feeds and return new articles"""
        all_articles = []
        existing_urls = self._load_existing_urls()
        
        for _, feed_row in self.feeds_df.iterrows():
            try:
                articles = self._crawl_single_feed(
                    feed_row['source'], 
                    feed_row['url'],
                    existing_urls
                )
                all_articles.extend(articles)
                time.sleep(1)  # Be polite to servers
            except Exception as e:
                self.logger.error(f"Error crawling {feed_row['source']}: {e}")
        
        return all_articles
    
    def _crawl_single_feed(self, source: str, url: str, existing_urls: set) -> List[Dict]:
        """Crawl a single RSS feed"""
        self.logger.info(f"Crawling {source}")
        
        # Parse RSS feed
        feed = feedparser.parse(url)
        
        if feed.bozo:
            self.logger.warning(f"Feed parsing issues for {source}: {feed.bozo_exception}")
        
        articles = []
        cutoff_date = datetime.now() - timedelta(days=7)  # Only last week
        
        for entry in feed.entries:
            try:
                # Skip if we've seen this URL
                if entry.link in existing_urls:
                    continue
                
                # Parse publication date
                pub_date = self._parse_date(entry)
                if pub_date and pub_date < cutoff_date:
                    continue
                
                article = {
                    'title': entry.title.strip(),
                    'description': self._extract_description(entry),
                    'link': entry.link,
                    'source': source,
                    'published': pub_date.isoformat() if pub_date else None,
                    'scraped_at': datetime.now().isoformat()
                }
                
                articles.append(article)
                
            except Exception as e:
                self.logger.error(f"Error processing entry from {source}: {e}")
        
        return articles
    
    def _parse_date(self, entry) -> Optional[datetime]:
        """Parse publication date from RSS entry"""
        from dateutil import parser as date_parser
        
        date_fields = ['published_parsed', 'updated_parsed']
        
        for field in date_fields:
            if hasattr(entry, field) and getattr(entry, field):
                try:
                    time_struct = getattr(entry, field)
                    return datetime(*time_struct[:6])
                except:
                    pass
        
        # Try parsing string dates
        string_fields = ['published', 'updated']
        for field in string_fields:
            if hasattr(entry, field):
                try:
                    return date_parser.parse(getattr(entry, field))
                except:
                    pass
        
        return None
    
    def _extract_description(self, entry) -> str:
        """Extract description/summary from entry"""
        if hasattr(entry, 'summary'):
            return self._clean_html(entry.summary)
        elif hasattr(entry, 'description'):
            return self._clean_html(entry.description)
        return ""
    
    def _clean_html(self, text: str) -> str:
        """Remove HTML tags and clean up text"""
        import re
        # Remove HTML tags
        clean = re.sub('<.*?>', '', text)
        # Clean up whitespace
        clean = re.sub(r'\s+', ' ', clean).strip()
        return clean
    
    def _load_existing_urls(self) -> set:
        """Load existing URLs to avoid duplicates"""
        try:
            df = pd.read_csv(self.memory_file)
            return set(df['link'].tolist())
        except FileNotFoundError:
            return set()
    
    def save_articles(self, articles: List[Dict]):
        """Save articles to memory file"""
        if not articles:
            return
        
        df = pd.DataFrame(articles)
        
        # Append to existing file or create new
        try:
            existing_df = pd.read_csv(self.memory_file)
            combined_df = pd.concat([existing_df, df], ignore_index=True)
        except FileNotFoundError:
            combined_df = df
        
        # Remove duplicates and save
        combined_df = combined_df.drop_duplicates(subset=['link'])
        combined_df.to_csv(self.memory_file, index=False)
        
        self.logger.info(f"Saved {len(articles)} new articles")
