#!/usr/bin/env python3
"""
Weekly Digest Email Sender for Jewish Studies Feed
Sends weekly digest emails to subscribers
"""

import os
import sys
from datetime import datetime
from src.email_sender import EmailSender

def main():
    """Main function to send weekly digest"""
    print(f"Starting weekly digest email at {datetime.now()}")
    
    # Check for required environment variables
    required_vars = ['EMAIL_USERNAME', 'EMAIL_PASSWORD', 'EMAIL_SUBSCRIBERS']
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    
    if missing_vars:
        print(f"ERROR: Missing required environment variables: {', '.join(missing_vars)}")
        sys.exit(1)
    
    try:
        # Initialize email sender
        sender = EmailSender()
        
        # Send weekly digest
        success = sender.send_weekly_digest(
            news_file='data/output/news_articles.json',
            research_file='data/output/research_articles.json'
        )
        
        if success:
            print("Weekly digest emails sent successfully!")
        else:
            print("Failed to send digest emails")
            sys.exit(1)
            
    except Exception as e:
        print(f"ERROR: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
