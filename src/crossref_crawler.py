import requests
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import time
import logging

class CrossrefCrawler:
    def __init__(self, journals_file: str, email: str):
        self.journals_df = pd.read_csv(journals_file)
        self.email = email
        self.logger = self._setup_logger()
        self.base_url = "https://api.crossref.org"
    
    def _setup_logger(self):
        logging.basicConfig(level=logging.INFO)
        return logging.getLogger(__name__)
    
    def crawl_journals(self, days_back: int = 14) -> List[Dict]:
        """Crawl all journals for recent articles"""
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=days_back)
        
        all_articles = []
        
        for _, journal in self.journals_df.iterrows():
            try:
                articles = self._crawl_journal(
                    journal['issn'], 
                    journal['journal_full'],
                    journal['journal_short'],
                    start_date, 
                    end_date
                )
                all_articles.extend(articles)
                time.sleep(1)  # Rate limiting
            except Exception as e:
                self.logger.error(f"Error crawling {journal['journal_full']}: {e}")
        
        return all_articles
    
    def _crawl_journal(self, issn: str, journal_full: str, journal_short: str, 
                      start_date, end_date) -> List[Dict]:
        """Crawl a single journal"""
        self.logger.info(f"Crawling {journal_full}")
        
        articles = []
        
        # Try both created and published date filters
        for date_type in ['created', 'published']:
            try:
                batch_articles = self._fetch_articles(
                    issn, start_date, end_date, date_type
                )
                
                for article in batch_articles:
                    article.update({
                        'issn': issn,
                        'journal_full': journal_full,
                        'journal_short': journal_short
                    })
                
                articles.extend(batch_articles)
                
            except Exception as e:
                self.logger.error(f"Error fetching {date_type} articles for {journal_full}: {e}")
        
        # Remove duplicates by URL
        seen_urls = set()
        unique_articles = []
        for article in articles:
            if article['url'] not in seen_urls:
                seen_urls.add(article['url'])
                unique_articles.append(article)
        
        return unique_articles
    
    def _fetch_articles(self, issn: str, start_date, end_date, date_type: str) -> List[Dict]:
        """Fetch articles from Crossref API"""
        url = f"{self.base_url}/journals/{issn}/works"
        
        params = {
            'filter': f'from-{date_type}-date:{start_date},until-{date_type}-date:{end_date}',
            'select': 'title,author,abstract,URL,created,published',
            'rows': 1000,
            'mailto': self.email
        }
        
        response = requests.get(url, params=params)
        response.raise_for_status()
        
        data = response.json()
        articles = []
        
        for item in data.get('message', {}).get('items', []):
            try:
                article = self._parse_crossref_item(item)
                if article:
                    articles.append(article)
            except Exception as e:
                self.logger.error(f"Error parsing article: {e}")
        
        return articles
    
    def _parse_crossref_item(self, item: dict) -> Optional[Dict]:
        """Parse a single Crossref item"""
        if not item.get('URL'):
            return None
        
        # Extract authors
        authors = []
        if 'author' in item:
            for author in item['author']:
                name_parts = []
                if 'given' in author:
                    name_parts.append(author['given'])
                if 'family' in author:
                    name_parts.append(author['family'])
                if name_parts:
                    authors.append(' '.join(name_parts))
        
        # Extract title
        title = item.get('title', [''])[0] if item.get('title') else ''
        
        # Extract abstract
        abstract = item.get('abstract', '')
        
        # Clean HTML from abstract and title
        title = self._clean_html(title)
        abstract = self._clean_html(abstract)
        
        return {
            'title': title,
            'authors': ', '.join(authors),
            'abstract': abstract,
            'url': item['URL'],
            'doi': self._extract_doi(item['URL']),
            'created': self._extract_date(item, 'created'),
            'published': self._extract_date(item, 'published')
        }
    
    def _clean_html(self, text: str) -> str:
        """Remove HTML tags and clean text"""
        if not text:
            return ''
        
        import re
        clean = re.sub('<.*?>', ' ', text)
        clean = re.sub(r'\s+', ' ', clean).strip()
        return clean
    
    def _extract_doi(self, url: str) -> str:
        """Extract DOI from URL"""
        import re
        return re.sub(r'https?://dx\.doi\.org/', '', url)
    
    def _extract_date(self, item: dict, date_type: str) -> Optional[str]:
        """Extract date from Crossref item"""
        if date_type in item and 'date-parts' in item[date_type]:
            date_parts = item[date_type]['date-parts'][0]
            if len(date_parts) >= 3:
                return f"{date_parts[0]:04d}-{date_parts[1]:02d}-{date_parts[2]:02d}"
        return None
