import os
import json
import smtplib
import logging
from datetime import datetime
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import List, Dict

class EmailSender:
    def __init__(self):
        self.smtp_server = "smtp.gmail.com"
        self.smtp_port = 587
        self.username = os.getenv('EMAIL_USERNAME')
        self.password = os.getenv('EMAIL_PASSWORD')
        
        if not self.username or not self.password:
            raise ValueError("EMAIL_USERNAME and EMAIL_PASSWORD environment variables required")
        
        # Load subscribers from environment
        subscribers_json = os.getenv('EMAIL_SUBSCRIBERS', '[]')
        try:
            self.subscribers = json.loads(subscribers_json)
        except json.JSONDecodeError:
            self.subscribers = []
        
        self.logger = self._setup_logger()
    
    def _setup_logger(self):
        logging.basicConfig(level=logging.INFO)
        return logging.getLogger(__name__)
    
    def send_weekly_digest(self, news_file: str, research_file: str) -> bool:
        """Send weekly digest email to all subscribers"""
        if not self.subscribers:
            self.logger.warning("No subscribers found")
            return True
        
        try:
            # Load content
            news_data = self._load_json(news_file)
            research_data = self._load_json(research_file)
            
            # Generate email content
            subject, html_content, text_content = self._generate_email_content(news_data, research_data)
            
            # Send to all subscribers
            success_count = 0
            for subscriber in self.subscribers:
                if self._send_single_email(subscriber, subject, html_content, text_content):
                    success_count += 1
            
            self.logger.info(f"Successfully sent digest to {success_count}/{len(self.subscribers)} subscribers")
            return success_count > 0
            
        except Exception as e:
            self.logger.error(f"Error sending weekly digest: {e}")
            return False
    
    def _load_json(self, file_path: str) -> Dict:
        """Load JSON file with error handling"""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            return {'articles_count': 0, 'all_articles': [], 'sources': {}}
    
    def _generate_email_content(self, news_data: Dict, research_data: Dict) -> tuple:
        """Generate email subject and content"""
        week_date = datetime.now().strftime('%B %d, %Y')
        news_count = news_data.get('articles_count', 0)
        research_count = research_data.get('articles_count', 0)
        
        subject = f"Jewish Studies Weekly Digest - {week_date}"
        
        # HTML content
        html_content = f"""
        <html>
        <body style="font-family: Arial, sans-serif; line-height: 1.6; color: #333;">
            <div style="max-width: 800px; margin: 0 auto;">
                <h1 style="color: #2c5aa0; border-bottom: 2px solid #2c5aa0; padding-bottom: 10px;">
                    Jewish Studies Weekly Digest
                </h1>
                <p style="color: #666; font-size: 14px;">Week of {week_date}</p>
                
                <h2 style="color: #1a365d;">📚 Academic Articles ({research_count})</h2>
                {self._format_research_articles_html(research_data)}
                
                <h2 style="color: #1a365d;">📰 Research News ({news_count})</h2>
                {self._format_news_articles_html(news_data)}
                
                <hr style="margin: 30px 0; border: 1px solid #eee;">
                <p style="color: #666; font-size: 12px; text-align: center;">
                    Jewish Studies Feed - Generated automatically from academic journals and news sources<br>
                    <a href="https://github.com/jballhalla/jewish-studies-feed">View on GitHub</a>
                </p>
            </div>
        </body>
        </html>
        """
        
        # Plain text content
        text_content = f"""
Jewish Studies Weekly Digest - {week_date}

ACADEMIC ARTICLES ({research_count}):
{self._format_research_articles_text(research_data)}

RESEARCH NEWS ({news_count}):
{self._format_news_articles_text(news_data)}

---
Jewish Studies Feed - https://github.com/jballhalla/jewish-studies-feed
        """
        
        return subject, html_content, text_content
    
    def _format_research_articles_html(self, research_data: Dict) -> str:
        """Format research articles for HTML email"""
        articles = research_data.get('all_articles', [])
        
        if not articles:
            return "<p><em>No new academic articles this week.</em></p>"
        
        html = ""
        for journal, data in research_data.get('journals', {}).items():
            html += f"<h3 style='color: #2d3748; margin-top: 25px;'>{journal} ({data['count']})</h3>"
            html += "<ul style='padding-left: 20px;'>"
            
            for article in data['articles'][:5]:  # Limit to 5 per journal
                authors = article.get('authors', 'Unknown')[:100] + ('...' if len(article.get('authors', '')) > 100 else '')
                html += f"""
                <li style='margin-bottom: 10px;'>
                    <strong><a href="{article.get('url', '#')}" style="color: #2c5aa0; text-decoration: none;">
                        {article.get('title', 'Untitled')}
                    </a></strong><br>
                    <span style='color: #666; font-size: 14px;'>{authors}</span>
                </li>
                """
            html += "</ul>"
        
        return html
    
    def _format_news_articles_html(self, news_data: Dict) -> str:
        """Format news articles for HTML email"""
        articles = news_data.get('all_articles', [])
        
        if not articles:
            return "<p><em>No research-relevant news this week.</em></p>"
        
        html = ""
        for source, data in news_data.get('sources', {}).items():
            html += f"<h3 style='color: #2d3748; margin-top: 25px;'>{source} ({data['count']})</h3>"
            html += "<ul style='padding-left: 20px;'>"
            
            for article in data['articles'][:5]:  # Limit to 5 per source
                description = article.get('description', '')[:150] + ('...' if len(article.get('description', '')) > 150 else '')
                html += f"""
                <li style='margin-bottom: 10px;'>
                    <strong><a href="{article.get('link', '#')}" style="color: #2c5aa0; text-decoration: none;">
                        {article.get('title', 'Untitled')}
                    </a></strong><br>
                    <span style='color: #666; font-size: 14px;'>{description}</span>
                </li>
                """
            html += "</ul>"
        
        return html
    
    def _format_research_articles_text(self, research_data: Dict) -> str:
        """Format research articles for plain text email"""
        articles = research_data.get('all_articles', [])
        
        if not articles:
            return "No new academic articles this week.\n"
        
        text = ""
        for journal, data in research_data.get('journals', {}).items():
            text += f"\n{journal} ({data['count']}):\n"
            for i, article in enumerate(data['articles'][:5], 1):
                text += f"{i}. {article.get('title', 'Untitled')}\n"
                text += f"   Authors: {article.get('authors', 'Unknown')[:100]}\n"
                text += f"   Link: {article.get('url', 'N/A')}\n\n"
        
        return text
    
    def _format_news_articles_text(self, news_data: Dict) -> str:
        """Format news articles for plain text email"""
        articles = news_data.get('all_articles', [])
        
        if not articles:
            return "No research-relevant news this week.\n"
        
        text = ""
        for source, data in news_data.get('sources', {}).items():
            text += f"\n{source} ({data['count']}):\n"
            for i, article in enumerate(data['articles'][:5], 1):
                text += f"{i}. {article.get('title', 'Untitled')}\n"
                text += f"   Link: {article.get('link', 'N/A')}\n\n"
        
        return text
    
    def _send_single_email(self, recipient: str, subject: str, html_content: str, text_content: str) -> bool:
        """Send email to a single recipient"""
        try:
            # Create message
            msg = MIMEMultipart('alternative')
            msg['From'] = self.username
            msg['To'] = recipient
            msg['Subject'] = subject
            
            # Attach both text and HTML versions
            text_part = MIMEText(text_content, 'plain', 'utf-8')
            html_part = MIMEText(html_content, 'html', 'utf-8')
            
            msg.attach(text_part)
            msg.attach(html_part)
            
            # Send email
            with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
                server.starttls()
                server.login(self.username, self.password)
                server.send_message(msg)
            
            self.logger.info(f"Successfully sent email to {recipient}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to send email to {recipient}: {e}")
            return False
