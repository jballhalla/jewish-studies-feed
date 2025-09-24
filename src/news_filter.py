import pandas as pd
import json
import os
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple
import anthropic
import time
import csv
from io import StringIO

class NewsFilter:
    def __init__(self, memory_file: str, output_file: str):
        self.memory_file = memory_file
        self.output_file = output_file
        self.logger = self._setup_logger()
        
        # Initialize Anthropic client
        api_key = os.getenv('ANTHROPIC_API_KEY')
        if not api_key:
            raise ValueError("ANTHROPIC_API_KEY environment variable not set")
        
        self.client = anthropic.Anthropic(api_key=api_key)
        self.model = "claude-3-5-haiku-latest"
        
        # Retry configuration
        self.max_retries = 3
        self.retry_delay = 5  # seconds
    
    def _setup_logger(self):
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        return logging.getLogger(__name__)
    
    def filter_weekly_news(self, days_back: int = 7) -> Tuple[List[Dict], int]:
        """
        Filter the last week's news articles for research relevance
        Returns: (filtered_articles, total_processed)
        """
        # Load recent articles
        recent_articles = self._load_recent_articles(days_back)
        
        if not recent_articles:
            self.logger.info("No articles to filter")
            return [], 0
        
        self.logger.info(f"Filtering {len(recent_articles)} articles from the last {days_back} days")
        
        # Save weekly backup before filtering
        self._save_weekly_backup(recent_articles)
        
        # Filter articles using Anthropic API
        filtered_articles = self._filter_with_anthropic(recent_articles)
        
        # Save filtered results to output
        self._save_filtered_output(filtered_articles)
        
        # Clean up memory file (remove processed articles)
        self._cleanup_memory_file(days_back)
        
        return filtered_articles, len(recent_articles)
    
    def _load_recent_articles(self, days_back: int) -> List[Dict]:
        """Load articles from the last N days"""
        try:
            df = pd.read_csv(self.memory_file)
            df['scraped_at'] = pd.to_datetime(df['scraped_at'])
            
            # Filter to last N days
            cutoff = datetime.now() - timedelta(days=days_back)
            recent_df = df[df['scraped_at'] >= cutoff]
            
            return recent_df.to_dict('records')
            
        except (FileNotFoundError, pd.errors.EmptyDataError):
            self.logger.warning("No news memory file found")
            return []
        except Exception as e:
            self.logger.error(f"Error loading recent articles: {e}")
            return []
    
    def _save_weekly_backup(self, articles: List[Dict]) -> None:
        """Save weekly backup of all articles before filtering"""
        if not articles:
            return
        
        # Create weekly backup filename with date
        backup_dir = os.path.dirname(self.memory_file)
        date_str = datetime.now().strftime('%Y-%m-%d')
        backup_file = os.path.join(backup_dir, f"news_weekly_{date_str}.csv")
        
        # Save backup
        df = pd.DataFrame(articles)
        df.to_csv(backup_file, index=False)
        
        self.logger.info(f"Saved weekly backup to {backup_file} with {len(articles)} articles")
    
    def _filter_with_anthropic(self, articles: List[Dict]) -> List[Dict]:
        """Filter articles using Anthropic API with retry logic"""
        if not articles:
            return []
        
        # Prepare articles for API call (limit fields to reduce token usage)
        simplified_articles = []
        for i, article in enumerate(articles):
            simplified_articles.append({
                'id': i,
                'title': article.get('title', ''),
                'description': article.get('description', ''),
                'source': article.get('source', ''),
                'link': article.get('link', ''),
                'published': article.get('published', ''),
                'author': article.get('author', '')
            })
        
        # Convert to JSON string for API
        articles_json = json.dumps(simplified_articles, indent=2)
        
        prompt = self._create_filter_prompt(articles_json)
        
        # Try API call with retries
        for attempt in range(self.max_retries):
            try:
                self.logger.info(f"Calling Anthropic API (attempt {attempt + 1}/{self.max_retries})")
                
                response = self.client.messages.create(
                    model=self.model,
                    max_tokens=4000,
                    temperature=0.1,  # Low temperature for consistent filtering
                    messages=[
                        {
                            "role": "user",
                            "content": prompt
                        }
                    ]
                )
                
                # Parse response
                filtered_ids = self._parse_api_response(response.content[0].text)
                
                if filtered_ids is None:
                    raise ValueError("Failed to parse API response")
                
                # Extract filtered articles
                filtered_articles = []
                for article_id in filtered_ids:
                    if 0 <= article_id < len(articles):
                        filtered_articles.append(articles[article_id])
                
                self.logger.info(f"Successfully filtered {len(articles)} articles down to {len(filtered_articles)} relevant ones")
                return filtered_articles
                
            except Exception as e:
                self.logger.error(f"Attempt {attempt + 1} failed: {e}")
                if attempt < self.max_retries - 1:
                    self.logger.info(f"Retrying in {self.retry_delay} seconds...")
                    time.sleep(self.retry_delay)
                else:
                    self.logger.error("All API attempts failed, returning empty results")
                    return []
        
        return []
    
    def _create_filter_prompt(self, articles_json: str) -> str:
        """Create the filtering prompt for Anthropic API"""
        return f"""You are helping curate a weekly digest for Jewish Studies researchers and academics. Your task is to filter news headlines to find only those that are relevant to academic research, studies, publications, and scholarly activities in Jewish Studies.

INCLUDE articles about:
- New academic publications, books, research papers
- Research studies and their findings
- Academic conferences, symposiums, lectures
- University programs, courses, academic appointments
- Research grants, fellowships, academic awards
- Policy reports and white papers
- Archaeological discoveries and scholarly analysis
- Museum exhibitions with scholarly/research focus
- Think tank reports and academic policy analysis
- Educational initiatives and curriculum development
- Academic collaborations and institutional partnerships

EXCLUDE articles about:
- General news, politics, current events
- Entertainment, sports, lifestyle
- Business news (unless about academic publishing/research funding)
- Obituaries (unless specifically about major scholars)
- Community events, religious services, cultural festivals
- Real estate, local community news
- Opinion pieces without research backing
- General Israeli/Middle East politics (unless research-focused)

Here are the news articles from this week:

{articles_json}

Please return ONLY a JSON array containing the ID numbers of articles that are relevant to Jewish Studies research and academia. For example: [0, 5, 12, 18]

If no articles are relevant, return an empty array: []

Response (JSON array only):"""
    
    def _parse_api_response(self, response_text: str) -> Optional[List[int]]:
        """Parse the API response to extract article IDs"""
        try:
            # Clean the response - remove any extra text and extract JSON
            response_text = response_text.strip()
            
            # Look for JSON array pattern
            import re
            json_match = re.search(r'\[[\d\s,]*\]', response_text)
            if json_match:
                json_str = json_match.group(0)
                article_ids = json.loads(json_str)
                
                # Validate that it's a list of integers
                if isinstance(article_ids, list) and all(isinstance(x, int) for x in article_ids):
                    return article_ids
            
            # Try parsing the whole response as JSON
            article_ids = json.loads(response_text)
            if isinstance(article_ids, list) and all(isinstance(x, int) for x in article_ids):
                return article_ids
            
            self.logger.error(f"Invalid response format: {response_text}")
            return None
            
        except json.JSONDecodeError as e:
            self.logger.error(f"JSON decode error: {e}, response: {response_text}")
            return None
        except Exception as e:
            self.logger.error(f"Error parsing API response: {e}")
            return None
    
    def _save_filtered_output(self, filtered_articles: List[Dict]) -> None:
        """Save filtered articles to output JSON"""
        # Create output structure similar to RSS crawler
        output = {
            'update': datetime.now().isoformat(),
            'articles_count': len(filtered_articles),
            'period_days': 7,
            'filter': 'research_relevant',
            'sources': {},
            'all_articles': filtered_articles
        }
        
        # Group by source for better organization
        if filtered_articles:
            for article in filtered_articles:
                source = article.get('source', 'Unknown')
                if source not in output['sources']:
                    output['sources'][source] = {
                        'count': 0,
                        'articles': []
                    }
                output['sources'][source]['articles'].append(article)
                output['sources'][source]['count'] += 1
        
        # Ensure output directory exists
        os.makedirs(os.path.dirname(self.output_file), exist_ok=True)
        
        # Save to JSON
        with open(self.output_file, 'w', encoding='utf-8') as f:
            json.dump(output, f, indent=2, ensure_ascii=False)
        
        self.logger.info(f"Saved {len(filtered_articles)} filtered articles to {self.output_file}")
    
    def _cleanup_memory_file(self, days_back: int) -> None:
        """Remove processed articles from memory file to prevent re-processing"""
        try:
            df = pd.read_csv(self.memory_file)
            df['scraped_at'] = pd.to_datetime(df['scraped_at'])
            
            # Keep only articles newer than the cutoff (i.e., remove the ones we just processed)
            cutoff = datetime.now() - timedelta(days=days_back)
            remaining_df = df[df['scraped_at'] < cutoff]
            
            # Also keep recent articles that weren't processed (safety margin)
            very_recent = datetime.now() - timedelta(hours=2)
            very_recent_df = df[df['scraped_at'] >= very_recent]
            
            # Combine remaining old articles with very recent ones
            if len(remaining_df) > 0 and len(very_recent_df) > 0:
                cleaned_df = pd.concat([remaining_df, very_recent_df], ignore_index=True)
            elif len(remaining_df) > 0:
                cleaned_df = remaining_df
            elif len(very_recent_df) > 0:
                cleaned_df = very_recent_df
            else:
                # Create empty dataframe with correct columns
                cleaned_df = pd.DataFrame(columns=df.columns)
            
            # Remove duplicates and sort
            cleaned_df = cleaned_df.drop_duplicates(subset=['link'], keep='first')
            cleaned_df = cleaned_df.sort_values('scraped_at', ascending=False)
            
            # Save cleaned file
            cleaned_df.to_csv(self.memory_file, index=False)
            
            processed_count = len(df) - len(cleaned_df)
            self.logger.info(f"Cleaned memory file: removed {processed_count} processed articles, {len(cleaned_df)} remaining")
            
        except Exception as e:
            self.logger.error(f"Error cleaning memory file: {e}")
