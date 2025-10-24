import pandas as pd
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import time
from crossref.restful import Works, Etiquette
import json
import os

class CrossrefCrawler:
    def __init__(self, journals_file: str, memory_file: str):
        self.journals_df = pd.read_csv(journals_file)
        self.active_journals = self.journals_df[self.journals_df['active'] == True]
        self.memory_file = memory_file
        self.logger = self._setup_logger()
        
        # Setup polite etiquette for Crossref API
        self.etiquette = Etiquette(
            'Jewish Studies Feed',
            '1.0',
            'https://github.com/jballhalla/jewish-studies-feed',
            'action@github.com'
        )
        self.works = Works(etiquette=self.etiquette)
        
        # Rate limiting: Crossref allows 50 requests/second, we'll be conservative
        self.request_delay = 0.1  # 100ms between requests = 10 requests/second
    
    def _setup_logger(self):
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        return logging.getLogger(__name__)
    
    def crawl_all_journals(self, days_back: int = 8) -> List[Dict]:
        """Crawl all active journals for new articles from last N days"""
        all_articles = []
        existing_dois = self._load_existing_dois()
        
        # Calculate date range - go back slightly more than a week to ensure no gaps
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days_back)
        
        self.logger.info(f"Crawling {len(self.active_journals)} journals for articles from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
        
        for _, journal_row in self.active_journals.iterrows():
            try:
                articles = self._crawl_single_journal(
                    journal_row['issn'], 
                    journal_row['journal_name'],
                    journal_row['journal_abbrev'],
                    existing_dois,
                    start_date,
                    end_date
                )
                all_articles.extend(articles)
                self.logger.info(f"Found {len(articles)} new articles from {journal_row['journal_name']}")
                
                # Rate limiting
                time.sleep(self.request_delay)
                
            except Exception as e:
                self.logger.error(f"Error crawling {journal_row['journal_name']} ({journal_row['issn']}): {e}")
                continue
        
        self.logger.info(f"Total new articles found: {len(all_articles)}")
        return all_articles
    
    def _crawl_single_journal(self, issn: str, journal_name: str, journal_abbrev: str, 
                             existing_dois: set, start_date: datetime, end_date: datetime) -> List[Dict]:
        """Crawl a single journal by ISSN"""
        articles = []
        
        try:
            # Format dates for Crossref API (YYYY-MM-DD)
            start_str = start_date.strftime('%Y-%m-%d')
            end_str = end_date.strftime('%Y-%m-%d')
            
            self.logger.info(f"Querying Crossref for {journal_name} (ISSN: {issn})")
            
            # Query Crossref API for works in date range
            # Using from_online_pub_date and until_online_pub_date filters
            query = self.works.filter(
                issn=issn,
                from_online_pub_date=start_str,
                until_online_pub_date=end_str
            ).select([
                'DOI', 'title', 'author', 'published-online', 'published-print',
                'abstract', 'URL', 'page', 'volume', 'issue', 'container-title',
                'subject', 'type', 'created'
            ])
            
            # Get count first to see if there are any results
            count = query.count()
            self.logger.info(f"Found {count} total articles for {journal_name} in date range")
            
            if count == 0:
                return articles
            
            # Iterate through results
            processed_count = 0
            for work in query:
                try:
                    # Skip if we've seen this DOI
                    doi = work.get('DOI', '').strip()
                    if not doi or doi in existing_dois:
                        continue
                    
                    # Extract article data
                    article = self._extract_article_data(work, journal_name, journal_abbrev, issn)
                    
                    if article:
                        articles.append(article)
                    
                    processed_count += 1
                    
                    # Progress logging for large result sets
                    if processed_count % 50 == 0:
                        self.logger.info(f"Processed {processed_count}/{count} articles for {journal_name}")
                    
                except Exception as e:
                    self.logger.error(f"Error processing work from {journal_name}: {e}")
                    continue
            
            self.logger.info(f"Extracted {len(articles)} new articles from {journal_name}")
            return articles
            
        except Exception as e:
            self.logger.error(f"Failed to query {journal_name}: {e}")
            return []
    
    def _extract_article_data(self, work: Dict, journal_name: str, journal_abbrev: str, issn: str) -> Optional[Dict]:
        """Extract and clean article data from Crossref work"""
        try:
            # Required fields
            doi = work.get('DOI', '').strip()
            if not doi:
                return None
            
            title = self._extract_title(work)
            if not title:
                return None
            
            # Extract authors
            authors = self._extract_authors(work)
            
            # Extract publication dates
            pub_date_online = self._extract_date(work.get('published-online'))
            pub_date_print = self._extract_date(work.get('published-print'))
            created_date = self._extract_date(work.get('created'))
            
            # Use online date, then print date, then created date
            pub_date = pub_date_online or pub_date_print or created_date
            
            # Extract abstract
            abstract = work.get('abstract', '').strip()
            if abstract:
                # Remove HTML tags if present
                import re
                abstract = re.sub(r'<[^>]+>', ' ', abstract)
                abstract = re.sub(r'\s+', ' ', abstract).strip()
            
            # Extract URL
            url = work.get('URL', '').strip()
            if not url and doi:
                url = f"https://doi.org/{doi}"
            
            # Extract volume/issue/pages
            volume = work.get('volume', '').strip()
            issue = work.get('issue', '').strip()
            pages = work.get('page', '').strip()
            
            # Extract subjects/keywords
            subjects = work.get('subject', [])
            if isinstance(subjects, list):
                subjects = '; '.join(subjects)
            else:
                subjects = str(subjects) if subjects else ''
            
            # Extract article type
            article_type = work.get('type', '').strip()
            
            article = {
                'doi': doi,
                'title': title,
                'authors': authors,
                'journal_name': journal_name,
                'journal_abbrev': journal_abbrev,
                'issn': issn,
                'volume': volume,
                'issue': issue,
                'pages': pages,
                'abstract': abstract,
                'url': url,
                'published_online': pub_date_online.isoformat() if pub_date_online else None,
                'published_print': pub_date_print.isoformat() if pub_date_print else None,
                'published_date': pub_date.isoformat() if pub_date else None,
                'subjects': subjects,
                'article_type': article_type,
                'scraped_at': datetime.now().isoformat()
            }
            
            return article
            
        except Exception as e:
            self.logger.error(f"Error extracting article data: {e}")
            return None
    
    def _extract_title(self, work: Dict) -> str:
        """Extract title from work"""
        title = work.get('title', [])
        if isinstance(title, list) and title:
            return title[0].strip()
        elif isinstance(title, str):
            return title.strip()
        return ''
    
    def _extract_authors(self, work: Dict) -> str:
        """Extract authors from work"""
        authors_list = work.get('author', [])
        if not authors_list:
            return ''
        
        author_names = []
        for author in authors_list:
            if isinstance(author, dict):
                given = author.get('given', '').strip()
                family = author.get('family', '').strip()
                if family:
                    if given:
                        author_names.append(f"{family}, {given}")
                    else:
                        author_names.append(family)
                elif given:
                    author_names.append(given)
        
        return '; '.join(author_names)
    
    def _extract_date(self, date_info: Optional[Dict]) -> Optional[datetime]:
        """Extract datetime from Crossref date format"""
        if not date_info or not isinstance(date_info, dict):
            return None
        
        date_parts = date_info.get('date-parts', [])
        if not date_parts or not isinstance(date_parts, list) or not date_parts[0]:
            return None
        
        parts = date_parts[0]
        
        try:
            year = parts[0] if len(parts) > 0 else None
            month = parts[1] if len(parts) > 1 else 1
            day = parts[2] if len(parts) > 2 else 1
            
            if year:
                return datetime(year, month, day)
        except (ValueError, IndexError, TypeError):
            pass
        
        return None
    
    def _load_existing_dois(self) -> set:
        """Load existing DOIs to avoid duplicates"""
        try:
            df = pd.read_csv(self.memory_file)
            return set(df['doi'].tolist())
        except (FileNotFoundError, pd.errors.EmptyDataError):
            self.logger.info("No existing research log found, starting fresh")
            return set()
        except Exception as e:
            self.logger.error(f"Error loading existing DOIs: {e}")
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
    
        # Remove duplicates based on DOI and save
        combined_df = combined_df.drop_duplicates(subset=['doi'], keep='first')
    
        # Sort by scraped_at descending to keep most recent first
        try:
            # FIX: Use format='ISO8601' with error handling
            combined_df['scraped_at'] = pd.to_datetime(combined_df['scraped_at'], format='ISO8601', errors='coerce')
            combined_df = combined_df.sort_values('scraped_at', ascending=False)
        except Exception as e:
            self.logger.warning(f"Error parsing datetime, using mixed format: {e}")
            combined_df['scraped_at'] = pd.to_datetime(combined_df['scraped_at'], format='mixed', errors='coerce')
            combined_df = combined_df.sort_values('scraped_at', ascending=False)

        # Keep only last 2 years to prevent file from growing too large
        two_years_ago = datetime.now() - timedelta(days=730)
        combined_df = combined_df[combined_df['scraped_at'] >= two_years_ago]
    
        # Save to CSV
        combined_df.to_csv(self.memory_file, index=False)
    
        self.logger.info(f"Saved {len(articles)} new articles. Total articles in memory: {len(combined_df)}")
    
    def generate_output_json(self, output_file: str, days_back: int = 7) -> None:
        """Generate JSON output for recent articles"""
        try:
            df = pd.read_csv(self.memory_file)
            # FIX: Use format='ISO8601' with error handling
            try:
                df['scraped_at'] = pd.to_datetime(df['scraped_at'], format='ISO8601', errors='coerce')
            except Exception as e:
                self.logger.warning(f"Error parsing datetime with ISO8601, using mixed format: {e}")
                df['scraped_at'] = pd.to_datetime(df['scraped_at'], format='mixed', errors='coerce')
            # Filter to last N days
            cutoff = datetime.now() - timedelta(days=days_back)
            recent_df = df[df['scraped_at'] >= cutoff]

            # Replace NaN with None before converting to dict
            #recent_df = recent_df.replace({pd.NA: None, pd.NaT: None, float('nan'): None})
            #recent_df = recent_df.where(pd.notna(recent_df), None)
            # **REPLACE NaN with None**
            recent_df = recent_df.astype(object).where(pd.notna(recent_df), None)

            # Group by journal for better organization
            output = {
                'update': datetime.now().isoformat(),
                'articles_count': len(recent_df),
                'period_days': days_back,
                'journals': {}
            }
            
            for journal in recent_df['journal_name'].unique():
                journal_articles = recent_df[recent_df['journal_name'] == journal]
                output['journals'][journal] = {
                    'count': len(journal_articles),
                    'articles': journal_articles.drop('scraped_at', axis=1).to_dict('records')
                }
        
            # Also include a flat list for easier processing
            output['all_articles'] = recent_df.drop('scraped_at', axis=1).to_dict('records')
            
            # Ensure output directory exists
            os.makedirs(os.path.dirname(output_file), exist_ok=True)
            
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(output, f, indent=2, ensure_ascii=False)
                
            self.logger.info(f"Generated output JSON with {len(recent_df)} articles from {len(output['journals'])} journals")
        
        except (FileNotFoundError, pd.errors.EmptyDataError):
            # Create empty output
            output = {
                'update': datetime.now().isoformat(),
                'articles_count': 0,
                'period_days': days_back,
                'journals': {},
                'all_articles': []
            }
            
            os.makedirs(os.path.dirname(output_file), exist_ok=True)
            
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(output, f, indent=2)
                
            self.logger.info("Generated empty output JSON")
