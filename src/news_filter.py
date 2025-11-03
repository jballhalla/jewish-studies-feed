import os
import pandas as pd
import json
import logging
import time
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple
from anthropic import Anthropic

class NewsFilter:
    def __init__(self, memory_file: str, output_file: str):
        self.memory_file = memory_file
        self.output_file = output_file
        self.logger = self._setup_logger()
        
        # Initialize Anthropic client
        api_key = os.getenv('ANTHROPIC_API_KEY')
        if not api_key:
            raise ValueError("ANTHROPIC_API_KEY environment variable is required")
        
        self.client = Anthropic(api_key=api_key)
        self.model = "claude-3-5-haiku-latest"
        
        # Retry configuration
        self.max_retries = 3
        self.retry_delay = 5  # seconds
        
        # Batch configuration - CRITICAL for handling large volumes
        self.batch_size = 100  # Process 100 articles at a time
    
    def _setup_logger(self):
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        return logging.getLogger(__name__)
    
    def filter_weekly_news(self, days_back: int = 7) -> Tuple[List[Dict], int]:
        """
        Filter news articles from the past week for research relevance
        Returns: (filtered_articles, total_processed)
        """
        # Load news articles from the past week
        weekly_articles = self._load_weekly_articles(days_back)
        
        if not weekly_articles:
            self.logger.info("No articles found for the past week")
            return [], 0
        
        self.logger.info(f"Processing {len(weekly_articles)} articles for research relevance")
        
        # Archive this week's articles before filtering
        self._archive_weekly_articles(weekly_articles)
        
        # Filter articles using Anthropic API (with batching)
        filtered_articles = self._filter_articles_with_ai_batched(weekly_articles)
        
        # Save filtered articles to output
        self._save_filtered_output(filtered_articles)
        
        # Clean up processed articles from memory
        self._cleanup_processed_articles(days_back)
        
        return filtered_articles, len(weekly_articles)
    
    def _load_weekly_articles(self, days_back: int) -> List[Dict]:
        """Load articles from the past week from memory file"""
        try:
            df = pd.read_csv(self.memory_file)
            
            # Parse scraped_at column with multiple date formats
            df['scraped_at'] = pd.to_datetime(df['scraped_at'], format='mixed', errors='coerce')
            
            # Filter to last N days
            cutoff_time = datetime.now() - timedelta(days=days_back)
            weekly_df = df[df['scraped_at'] >= cutoff_time]
            
            self.logger.info(f"Found {len(weekly_df)} articles from the past {days_back} days")
            
            return weekly_df.to_dict('records')
            
        except (FileNotFoundError, pd.errors.EmptyDataError):
            self.logger.warning("No news log file found or file is empty")
            return []
        except Exception as e:
            self.logger.error(f"Error loading weekly articles: {e}")
            return []
    
    def _archive_weekly_articles(self, articles: List[Dict]) -> None:
        """Archive this week's articles to a dated memory file"""
        if not articles:
            return
        
        # Create weekly archive filename
        week_start = datetime.now().strftime('%Y-%m-%d')
        archive_file = f"data/memory/news_archive_{week_start}.csv"
        
        try:
            # Ensure directory exists
            os.makedirs(os.path.dirname(archive_file), exist_ok=True)
            
            # Save to archive
            df = pd.DataFrame(articles)
            df.to_csv(archive_file, index=False)
            
            self.logger.info(f"Archived {len(articles)} articles to {archive_file}")
            
        except Exception as e:
            self.logger.error(f"Error archiving weekly articles: {e}")
    
    def _filter_articles_with_ai_batched(self, articles: List[Dict]) -> List[Dict]:
        """Filter articles using Anthropic API in batches"""
        if not articles:
            return []
        
        all_filtered_articles = []
        total_batches = (len(articles) + self.batch_size - 1) // self.batch_size
        
        self.logger.info(f"Processing {len(articles)} articles in {total_batches} batches of {self.batch_size}")
        
        for batch_num in range(total_batches):
            start_idx = batch_num * self.batch_size
            end_idx = min(start_idx + self.batch_size, len(articles))
            batch = articles[start_idx:end_idx]
            
            self.logger.info(f"Processing batch {batch_num + 1}/{total_batches} ({len(batch)} articles)")
            
            # Prepare articles for AI processing
            articles_for_ai = self._prepare_articles_for_ai(batch, start_idx)
            
            # Create the filtering prompt
            prompt = self._create_filtering_prompt(articles_for_ai)
            
            # Call Anthropic API with retry logic
            filtered_indices = self._call_anthropic_api(prompt)
            
            # Extract filtered articles based on AI response
            for idx in filtered_indices:
                # Convert batch-relative index to absolute index
                absolute_idx = idx
                if 0 <= absolute_idx < len(batch):
                    all_filtered_articles.append(batch[absolute_idx])
            
            # Rate limiting between batches
            if batch_num < total_batches - 1:
                time.sleep(2)  # 2 second delay between batches
        
        self.logger.info(f"AI filtered {len(all_filtered_articles)} research-relevant articles from {len(articles)} total")
        
        return all_filtered_articles
    
    def _prepare_articles_for_ai(self, articles: List[Dict], start_index: int = 0) -> List[Dict]:
        """Prepare articles for AI processing by keeping only essential fields"""
        prepared = []
        for i, article in enumerate(articles):
            # Handle None/NaN values
            title = article.get('title') or ''
            description = article.get('description') or ''
            source = article.get('source') or ''
            
            # Ensure they're strings before slicing
            title = str(title) if title else ''
            description = str(description) if description else ''
            source = str(source) if source else ''
            
            prepared.append({
                'index': i,  # Batch-relative index
                'title': title,
                'description': description[:300],  # Limit description length
                'source': source
            })
        return prepared
    
    def _create_filtering_prompt(self, articles: List[Dict]) -> str:
        """Create the prompt for AI filtering"""
        articles_text = []
        for article in articles:
            # Truncate long descriptions to save tokens
            desc = article['description'][:200] if article['description'] else ''
            
            articles_text.append(
                f"Index: {article['index']}\n"
                f"Title: {article['title']}\n"
                f"Description: {desc}\n"
                f"Source: {article['source']}\n"
                f"---"
            )
        
        articles_str = "\n".join(articles_text)
        
        prompt = f"""You are helping to curate a weekly digest for the Berman Archive. Your task is to identify articles that are relevant to research and scholarship in Jewish Studies.

INCLUDE articles that mention or discuss:
- New academic research, studies, or publications in Jewish Studies
- Reports, white papers, or research studies related to Jewish communities
- Academic conferences, symposia, or scholarly events
- New books, journal articles, or academic publications
- Research findings, surveys, or data analysis about Jewish communities
- Academic appointments, fellowships, or scholarly achievements
- Educational initiatives, curricula, or academic programs
- Think tank reports or policy analysis
- Scholarly commentary and analysis
- Research grants or funding announcements for Jewish Studies

EXCLUDE articles about:
- General news, politics, or current events (unless they include research/academic analysis)
- Opinion pieces without research backing
- Community events or social activities
- Religious ceremonies or practices (unless academic/research focused)
- Business news or financial reports
- Entertainment, sports, or lifestyle content
- Breaking news or daily political developments

In sum, INCLUDE articles based on research and scholarship, and EXCLUDE articles that are not relevant to Jewish Studies.

Here are {len(articles)} articles to evaluate:

{articles_str}

Please respond with ONLY a JSON array containing the index numbers of articles that meet the research/academic criteria. For example: [0, 5, 12, 23]

Do not include any other text in your response, just the JSON array.
"""
        
        return prompt
    
    def _call_anthropic_api(self, prompt: str) -> List[int]:
        """Call Anthropic API with retry logic"""
        for attempt in range(self.max_retries):
            try:
                self.logger.info(f"Calling Anthropic API (attempt {attempt + 1}/{self.max_retries})")
                
                response = self.client.messages.create(
                    model=self.model,
                    max_tokens=2000,  # Increased for larger batches
                    temperature=0.1,
                    messages=[
                        {
                            "role": "user",
                            "content": prompt
                        }
                    ]
                )
                
                # Extract the response content
                content = response.content[0].text.strip()
                
                # Try to find JSON array even if there's extra text
                import re
                json_match = re.search(r'\[[\d,\s]+\]', content)
                if json_match:
                    content = json_match.group(0)
                
                # Parse JSON response
                try:
                    filtered_indices = json.loads(content)
                    
                    # Validate that it's a list of integers
                    if isinstance(filtered_indices, list) and all(isinstance(x, int) for x in filtered_indices):
                        self.logger.info(f"API returned {len(filtered_indices)} filtered article indices")
                        return filtered_indices
                    else:
                        raise ValueError("Response is not a list of integers")
                
                except (json.JSONDecodeError, ValueError) as e:
                    self.logger.warning(f"Invalid JSON response on attempt {attempt + 1}: {e}")
                    self.logger.warning(f"Response content: {content[:500]}...")
                    
                    if attempt == self.max_retries - 1:
                        self.logger.error("Max retries reached, returning empty result")
                        return []
                    
                    # Wait before retrying
                    time.sleep(self.retry_delay)
                    continue
            
            except Exception as e:
                self.logger.error(f"API call failed on attempt {attempt + 1}: {e}")
                
                if attempt == self.max_retries - 1:
                    self.logger.error("Max retries reached, returning empty result")
                    return []
                
                # Wait before retrying
                time.sleep(self.retry_delay)
        
        return []
    
    def _save_filtered_output(self, filtered_articles: List[Dict]) -> None:
        """Save filtered articles to output JSON file"""
        try:
            # Ensure output directory exists
            os.makedirs(os.path.dirname(self.output_file), exist_ok=True)
            
            # Robust datetime/pandas object handling
            processed_articles = []
            for article in filtered_articles:
                processed_article = {}
                for key, value in article.items():
                    if pd.isna(value) or value is None:
                        processed_article[key] = None
                    elif isinstance(value, (pd.Timestamp, datetime)):
                        processed_article[key] = value.isoformat()
                    elif hasattr(value, 'isoformat') and callable(getattr(value, 'isoformat')):
                        processed_article[key] = value.isoformat()
                    else:
                        # Convert any remaining problematic types to string
                        try:
                            json.dumps(value)  # Test if it's JSON serializable
                            processed_article[key] = value
                        except (TypeError, ValueError):
                            processed_article[key] = str(value)
                processed_articles.append(processed_article)
            
            # Create output structure
            output = {
                'update': datetime.now().isoformat(),
                'articles_count': len(processed_articles),
                'filtered_date': datetime.now().strftime('%Y-%m-%d'),
                'week_start': (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d'),
                'has_articles': len(processed_articles) > 0,
                'sources': {},
                'all_articles': processed_articles
            }
            
            # Add a message if no articles found
            if not processed_articles:
                output['message'] = "No research-relevant articles found for this week."
                self.logger.info("No research-relevant articles found this week")
            else:
                # Group by source
                for article in processed_articles:
                    source = article.get('source', 'Unknown')
                    if source not in output['sources']:
                        output['sources'][source] = {
                            'count': 0,
                            'articles': []
                        }
                    output['sources'][source]['count'] += 1
                    output['sources'][source]['articles'].append(article)
            
            # Save to JSON file
            with open(self.output_file, 'w', encoding='utf-8') as f:
                json.dump(output, f, indent=2, ensure_ascii=False)
            
            if processed_articles:
                self.logger.info(f"Saved {len(processed_articles)} filtered articles to {self.output_file}")
            else:
                self.logger.info(f"Saved empty weekly digest to {self.output_file}")
        
        except Exception as e:
            self.logger.error(f"Error saving filtered output: {e}")
            import traceback
            self.logger.error(f"Full traceback: {traceback.format_exc()}")
        
    def _cleanup_processed_articles(self, days_back: int) -> None:
        """Remove processed articles from memory to keep file size manageable"""
        try:
            df = pd.read_csv(self.memory_file)
            
            # Parse dates
            df['scraped_at'] = pd.to_datetime(df['scraped_at'], format='mixed', errors='coerce')
            
            # Keep only articles newer than our processing window
            cutoff_time = datetime.now() - timedelta(days=days_back)
            remaining_df = df[df['scraped_at'] < cutoff_time]
            
            # Also keep recent articles (last 2 days) to avoid gaps
            recent_cutoff = datetime.now() - timedelta(days=2)
            recent_df = df[df['scraped_at'] >= recent_cutoff]
            
            # Combine remaining old articles with recent articles
            if not remaining_df.empty and not recent_df.empty:
                final_df = pd.concat([remaining_df, recent_df], ignore_index=True)
            elif not recent_df.empty:
                final_df = recent_df
            else:
                final_df = pd.DataFrame(columns=df.columns)
        
            # Remove duplicates and sort
            final_df = final_df.drop_duplicates(subset=['link'], keep='first')
            final_df = final_df.sort_values('scraped_at', ascending=False)
            
            # Save cleaned memory file
            final_df.to_csv(self.memory_file, index=False)
            
            original_count = len(df)
            final_count = len(final_df)
            processed_count = original_count - final_count
            
            self.logger.info(f"Cleaned memory file: removed {processed_count} processed articles, {final_count} articles remaining")
        
        except Exception as e:
            self.logger.error(f"Error cleaning up processed articles: {e}")
