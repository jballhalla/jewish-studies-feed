import feedparser
import pandas as pd
import requests
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import logging
import time
import re
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse

class RSSCrawler:
    def __init__(self, feeds_file: str, memory_file: str):
        self.feeds_df = pd.read_csv(feeds_file)
        self.memory_file = memory_file
        self.logger = self._setup_logger()
        # Set user agent to avoid blocking
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (compatible; JewishStudiesFeed/1.0; +https://github.com/jballhalla/jewish-studies-feed)'
        }
    
    def _setup_logger(self):
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        return logging.getLogger(__name__)
    
    def crawl_all_feeds(self, hours_back: int = 25) -> List[Dict]:
        """Crawl all RSS feeds and return new articles from last N hours"""
        all_articles = []
        existing_urls = self._load_existing_urls()
        cutoff_time = datetime.now() - timedelta(hours=hours_back)
        
        self.logger.info(f"Crawling {len(self.feeds_df)} RSS feeds for articles since {cutoff_time}")
        
        for _, feed_row in self.feeds_df.iterrows():
            try:
                articles = self._crawl_single_feed(
                    feed_row['source'], 
                    feed_row['url'],
                    existing_urls,
                    cutoff_time
                )
                all_articles.extend(articles)
                self.logger.info(f"Found {len(articles)} new articles from {feed_row['source']}")
                
                # Be respectful - 2 second delay between feeds
                time.sleep(2)
                
            except Exception as e:
                self.logger.error(f"Error crawling {feed_row['source']}: {e}")
                continue
        
        self.logger.info(f"Total new articles found: {len(all_articles)}")
        return all_articles
    
    def _crawl_single_feed(self, source: str, url: str, existing_urls: set, cutoff_time: datetime) -> List[Dict]:
        """Crawl a single RSS feed with robust error handling"""
        self.logger.info(f"Crawling {source}: {url}")
        
        try:
            # First try with feedparser (handles most cases)
            feed = feedparser.parse(url, request_headers=self.headers)
            
            # Check if feed parsed successfully
            if feed.bozo and not hasattr(feed, 'entries'):
                # Fallback: try direct HTTP request
                self.logger.warning(f"Feedparser failed for {source}, trying direct request")
                return self._crawl_with_requests(source, url, existing_urls, cutoff_time)
            
            if feed.bozo:
                self.logger.warning(f"Feed has parsing issues for {source}: {feed.bozo_exception}")
            
            articles = []
            
            for entry in feed.entries:
                try:
                    # Skip if we've seen this URL
                    entry_url = self._get_entry_url(entry)
                    if not entry_url or entry_url in existing_urls:
                        continue
                    
                    # Parse publication date
                    pub_date = self._parse_entry_date(entry)
                    
                    # Skip old articles (but include if no date found to be safe)
                    if pub_date and pub_date < cutoff_time:
                        continue
                    
                    # Extract article data
                    article = {
                        'title': self._get_entry_title(entry),
                        'description': self._get_entry_description(entry),
                        'link': entry_url,
                        'source': source,
                        'author': self._get_entry_author(entry),
                        'published': pub_date.isoformat() if pub_date else None,
                        'scraped_at': datetime.now().isoformat(),
                        'guid': getattr(entry, 'id', entry_url)
                    }
                    
                    # Only add if we have at least title and URL
                    if article['title'] and article['link']:
                        articles.append(article)
                    
                except Exception as e:
                    self.logger.error(f"Error processing entry from {source}: {e}")
                    continue
            
            return articles
            
        except Exception as e:
            self.logger.error(f"Failed to parse feed {source}: {e}")
            return []
    
    def _crawl_with_requests(self, source: str, url: str, existing_urls: set, cutoff_time: datetime) -> List[Dict]:
        """Fallback method using direct HTTP request and BeautifulSoup"""
        try:
            response = requests.get(url, headers=self.headers, timeout=30)
            response.raise_for_status()
            
            # Try to parse as XML/RSS
            soup = BeautifulSoup(response.content, 'xml')
            items = soup.find_all('item')
            
            if not items:
                # Maybe it's HTML disguised as RSS
                soup = BeautifulSoup(response.content, 'html.parser')
                items = soup.find_all('item')
            
            articles = []
            
            for item in items:
                try:
                    title_elem = item.find('title')
                    link_elem = item.find('link')
                    
                    if not title_elem or not link_elem:
                        continue
                    
                    title = title_elem.get_text(strip=True)
                    link = link_elem.get_text(strip=True)
                    
                    if not link or link in existing_urls:
                        continue
                    
                    # Try to get description
                    desc_elem = item.find('description') or item.find('summary')
                    description = desc_elem.get_text(strip=True) if desc_elem else ""
                    
                    # Try to get date
                    date_elem = item.find('pubDate') or item.find('published')
                    pub_date = None
                    if date_elem:
                        pub_date = self._parse_date_string(date_elem.get_text(strip=True))
                    
                    if pub_date and pub_date < cutoff_time:
                        continue
                    
                    # Try to get author
                    author_elem = item.find('author') or item.find('dc:creator')
                    author = author_elem.get_text(strip=True) if author_elem else ""
                    
                    article = {
                        'title': self._clean_text(title),
                        'description': self._clean_html(description),
                        'link': link.strip(),
                        'source': source,
                        'author': author,
                        'published': pub_date.isoformat() if pub_date else None,
                        'scraped_at': datetime.now().isoformat(),
                        'guid': link.strip()
                    }
                    
                    articles.append(article)
                    
                except Exception as e:
                    self.logger.error(f"Error processing XML item from {source}: {e}")
                    continue
            
            return articles
            
        except Exception as e:
            self.logger.error(f"Fallback parsing failed for {source}: {e}")
            return []
    
    def _get_entry_url(self, entry) -> Optional[str]:
        """Extract URL from RSS entry"""
        if hasattr(entry, 'link') and entry.link:
            return entry.link.strip()
        elif hasattr(entry, 'links') and entry.links:
            for link in entry.links:
                if link.get('rel') == 'alternate' or link.get('type') == 'text/html':
                    return link.href.strip()
            return entry.links[0].href.strip()
        return None
    
    def _get_entry_title(self, entry) -> str:
        """Extract title from RSS entry"""
        if hasattr(entry, 'title') and entry.title:
            return self._clean_text(entry.title)
        return "Untitled"
    
    def _get_entry_description(self, entry) -> str:
        """Extract description from RSS entry"""
        # Try multiple fields
        for field in ['summary', 'description', 'content']:
            if hasattr(entry, field):
                content = getattr(entry, field)
                if isinstance(content, list) and content:
                    content = content[0]
                if hasattr(content, 'value'):
                    content = content.value
                if content:
                    return self._clean_html(str(content))
        return ""
    
    def _get_entry_author(self, entry) -> str:
        """Extract author from RSS entry"""
        if hasattr(entry, 'author') and entry.author:
            return entry.author.strip()
        elif hasattr(entry, 'authors') and entry.authors:
            return ", ".join([author.name for author in entry.authors if hasattr(author, 'name')])
        return ""
    
    def _parse_entry_date(self, entry) -> Optional[datetime]:
        """Parse publication date from RSS entry"""
        # Try parsed date fields first
        for field in ['published_parsed', 'updated_parsed']:
            if hasattr(entry, field) and getattr(entry, field):
                try:
                    time_struct = getattr(entry, field)
                    return datetime(*time_struct[:6])
                except (TypeError, ValueError):
                    pass
        
        # Try string date fields
        for field in ['published', 'updated']:
            if hasattr(entry, field) and getattr(entry, field):
                date_str = getattr(entry, field)
                parsed_date = self._parse_date_string(date_str)
                if parsed_date:
                    return parsed_date
        
        return None
    
    def _parse_date_string(self, date_str: str) -> Optional[datetime]:
        """Parse date string using multiple methods"""
        if not date_str:
            return None
            
        try:
            from dateutil import parser as date_parser
            return date_parser.parse(date_str)
        except Exception:
            # Try common RSS date formats
            formats = [
                '%a, %d %b %Y %H:%M:%S %z',
                '%a, %d %b %Y %H:%M:%S %Z',
                '%Y-%m-%dT%H:%M:%S%z',
                '%Y-%m-%d %H:%M:%S',
                '%Y-%m-%d'
            ]
            
            for fmt in formats:
                try:
                    return datetime.strptime(date_str.strip(), fmt)
                except ValueError:
                    continue
        
        return None
    
    def _clean_html(self, text: str) -> str:
        """Remove HTML tags and clean up text"""
        if not text:
            return ""
        
        # Remove HTML tags
        clean = re.sub(r'<[^>]+>', ' ', text)
        # Clean up whitespace
        clean = re.sub(r'\s+', ' ', clean)
        # Remove common HTML entities
        clean = clean.replace('&nbsp;', ' ').replace('&amp;', '&')
        clean = clean.replace('&lt;', '<').replace('&gt;', '>')
        clean = clean.replace('&quot;', '"').replace('&#39;', "'")
        
        return clean.strip()
    
    def _clean_text(self, text: str) -> str:
        """Clean up text content"""
        if not text:
            return ""
        
        # Remove extra whitespace
        clean = re.sub(r'\s+', ' ', text)
        return clean.strip()
    
    def _load_existing_urls(self) -> set:
        """Load existing URLs to avoid duplicates"""
        try:
            df = pd.read_csv(self.memory_file)
            return set(df['link'].tolist())
        except (FileNotFoundError, pd.errors.EmptyDataError):
            self.logger.info("No existing news log found, starting fresh")
            return set()
        except Exception as e:
            self.logger.error(f"Error loading existing URLs: {e}")
            return set()
    
    def save_articles(self, articles: List[Dict]) -> None:
        """Save articles to memory file"""
        if not articles:
            self.logger.info("No new articles to save")
            return
    
        # Create DataFrame
        new_df = pd.DataFrame(articles)
    
        # Load existing data and combine
        try:
            existing_df = pd.read_csv(self.memory_file)
            combined_df = pd.concat([existing_df, new_df], ignore_index=True)
        except (FileNotFoundError, pd.errors.EmptyDataError):
            combined_df = new_df
    
        # Remove duplicates based on link and save
        combined_df = combined_df.drop_duplicates(subset=['link'], keep='first')

        # Sort by scraped_at descending to keep most recent first
        # FIX: Use mixed format to handle both ISO and other formats
        combined_df['scraped_at'] = pd.to_datetime(combined_df['scraped_at'], format='mixed', errors='coerce')
        combined_df = combined_df.sort_values('scraped_at', ascending=False)
    
        # Keep only last 30 days to prevent file from growing too large
        thirty_days_ago = datetime.now() - timedelta(days=30)
        combined_df = combined_df[combined_df['scraped_at'] >= thirty_days_ago]
    
        # Save to CSV
        combined_df.to_csv(self.memory_file, index=False)
    
        self.logger.info(f"Saved {len(articles)} new articles. Total articles in memory: {len(combined_df)}")
    
    def generate_output_json(self, output_file: str, days_back: int = 7) -> None:
        """Generate JSON output for recent articles"""
        try:
            df = pd.read_csv(self.memory_file)
            # Use format='ISO8601' to handle ISO datetime strings properly
            df['scraped_at'] = pd.to_datetime(df['scraped_at'], format='ISO8601')
    
            # Filter to last N days
            cutoff = datetime.now() - timedelta(days=days_back)
            recent_df = df[df['scraped_at'] >= cutoff]
    
            # Group by source for better organization
            output = {
                'update': datetime.now().isoformat(),
                'articles_count': len(recent_df),
                'sources': {}
            }
    
            for source in recent_df['source'].unique():
                source_articles = recent_df[recent_df['source'] == source]
                output['sources'][source] = {
                    'count': len(source_articles),
                    'articles': source_articles.drop('scraped_at', axis=1).to_dict('records')
                }
    
            # Also include a flat list for easier processing
            output['all_articles'] = recent_df.drop('scraped_at', axis=1).to_dict('records')
    
            # Ensure output directory exists
            import os
            os.makedirs(os.path.dirname(output_file), exist_ok=True)
    
            import json
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(output, f, indent=2, ensure_ascii=False)
        
            self.logger.info(f"Generated output JSON with {len(recent_df)} articles")
        
        except (FileNotFoundError, pd.errors.EmptyDataError):
            # Create empty output
            output = {
                'update': datetime.now().isoformat(),
                'articles_count': 0,
                'sources': {},
                'all_articles': []
            }
        
            import json
            import os
            os.makedirs(os.path.dirname(output_file), exist_ok=True)
        
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(output, f, indent=2)
            
            self.logger.info("Generated empty output JSON")
