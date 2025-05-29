
"""
Global News RSS Scraper
========================

A comprehensive web scraping solution for extracting news from RSS feeds 
across multiple countries and news agencies.

Author: RSS News Scraper
Date: 2025
"""

import feedparser
import requests
import pandas as pd
import sqlite3
import json
import csv
from datetime import datetime, timedelta
from urllib.parse import urljoin, urlparse
import time
import logging
from typing import List, Dict, Optional
import re
from dataclasses import dataclass
import hashlib
from concurrent.futures import ThreadPoolExecutor, as_completed
import langdetect
from langdetect import detect
import schedule
import threading

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('news_scraper.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

@dataclass
class NewsArticle:
    """Data class for storing news article information"""
    title: str
    publication_date: str
    source: str
    country: str
    summary: str
    url: str
    language: str = "unknown"
    category: str = "general"
    
    def to_dict(self) -> Dict:
        """Convert to dictionary for easy serialization"""
        return {
            'title': self.title,
            'publication_date': self.publication_date,
            'source': self.source,
            'country': self.country,
            'summary': self.summary,
            'url': self.url,
            'language': self.language,
            'category': self.category
        }
    
    def get_hash(self) -> str:
        """Generate unique hash for duplicate detection"""
        content = f"{self.title}{self.url}{self.publication_date}"
        return hashlib.md5(content.encode('utf-8')).hexdigest()

class GlobalNewsRSSConfig:
    """Configuration class containing RSS feeds for different countries"""
    
    RSS_FEEDS = {
        # English Speaking Countries
        'United Kingdom': [
            {'name': 'BBC News', 'url': 'http://feeds.bbci.co.uk/news/rss.xml'},
            {'name': 'The Guardian', 'url': 'https://www.theguardian.com/world/rss'},
            {'name': 'Reuters UK', 'url': 'https://feeds.reuters.com/reuters/UKdomesticNews'},
            {'name': 'Sky News', 'url': 'https://feeds.skynews.com/feeds/rss/home.xml'}
        ],
        'United States': [
            {'name': 'CNN', 'url': 'http://rss.cnn.com/rss/edition.rss'},
            {'name': 'Reuters US', 'url': 'https://feeds.reuters.com/reuters/domesticNews'},
            {'name': 'Associated Press', 'url': 'https://feeds.apnews.com/rss/apf-topnews'},
            {'name': 'NPR', 'url': 'https://feeds.npr.org/1001/rss.xml'}
        ],
        'Canada': [
            {'name': 'CBC News', 'url': 'https://www.cbc.ca/cmlink/rss-topstories'},
            {'name': 'CTV News', 'url': 'https://www.ctvnews.ca/rss/ctvnews-ca-top-stories-public-rss-1.822009'}
        ],
        'Australia': [
            {'name': 'ABC News Australia', 'url': 'https://www.abc.net.au/news/feed/1534/rss.xml'},
            {'name': 'The Australian', 'url': 'https://www.theaustralian.com.au/rss/'}
        ],
        
        # Asian Countries
        'Japan': [
            {'name': 'NHK World', 'url': 'https://www3.nhk.or.jp/rss/news/cat0.xml'},
            {'name': 'Japan Times', 'url': 'https://www.japantimes.co.jp/news/feed/'}
        ],
        'China': [
            {'name': 'China Daily', 'url': 'http://www.chinadaily.com.cn/rss/china_rss.xml'},
            {'name': 'Global Times', 'url': 'https://www.globaltimes.cn/rss/outbrain.xml'}
        ],
        'India': [
            {'name': 'Times of India', 'url': 'https://timesofindia.indiatimes.com/rssfeedstopstories.cms'},
            {'name': 'The Hindu', 'url': 'https://www.thehindu.com/news/national/feeder/default.rss'},
            {'name': 'NDTV', 'url': 'https://feeds.feedburner.com/ndtvnews-top-stories'}
        ],
        'South Korea': [
            {'name': 'Korea Herald', 'url': 'http://www.koreaherald.com/rss/020000000000.xml'},
            {'name': 'Yonhap News', 'url': 'https://en.yna.co.kr/RSS/news.xml'}
        ],
        'Singapore': [
            {'name': 'Straits Times', 'url': 'https://www.straitstimes.com/news/singapore/rss.xml'},
            {'name': 'Channel NewsAsia', 'url': 'https://www.channelnewsasia.com/api/v1/rss-outbound-feed?_format=xml'}
        ],
        'Malaysia': [
            {'name': 'The Star Malaysia', 'url': 'https://www.thestar.com.my/news/nation.rss'},
            {'name': 'New Straits Times', 'url': 'https://www.nst.com.my/news/nation/rss'}
        ],
        'Indonesia': [
            {'name': 'Jakarta Post', 'url': 'https://www.thejakartapost.com/rss'},
            {'name': 'Antara News', 'url': 'https://en.antaranews.com/rss/terkini.xml'}
        ],
        
        # Middle East and Africa
        'Middle East': [
            {'name': 'Al Jazeera', 'url': 'https://www.aljazeera.com/xml/rss/all.xml'},
            {'name': 'Al Arabiya', 'url': 'https://english.alarabiya.net/rss.xml'}
        ],
        'Israel': [
            {'name': 'Haaretz', 'url': 'https://www.haaretz.com/cmlink/1.628752'},
            {'name': 'Jerusalem Post', 'url': 'https://www.jpost.com/rss/rssfeedsheadlines.aspx'}
        ],
        'South Africa': [
            {'name': 'News24', 'url': 'https://feeds.news24.com/articles/news24/TopStories/rss'},
            {'name': 'IOL News', 'url': 'https://www.iol.co.za/cmlink/1.730626'}
        ],
        
        # European Countries
        'Germany': [
            {'name': 'Deutsche Welle', 'url': 'https://rss.dw.com/rdf/rss-en-all'},
            {'name': 'The Local Germany', 'url': 'https://www.thelocal.de/feed/'}
        ],
        'France': [
            {'name': 'France 24', 'url': 'https://www.france24.com/en/rss'},
            {'name': 'The Local France', 'url': 'https://www.thelocal.fr/feed/'}
        ],
        'Italy': [
            {'name': 'ANSA', 'url': 'https://www.ansa.it/english/news/rss.xml'},
            {'name': 'The Local Italy', 'url': 'https://www.thelocal.it/feed/'}
        ],
        'Spain': [
            {'name': 'El País English', 'url': 'https://feeds.elpais.com/mrss-s/pages/ep/site/english.elpais.com/portada'},
            {'name': 'The Local Spain', 'url': 'https://www.thelocal.es/feed/'}
        ],
        'Netherlands': [
            {'name': 'DutchNews.nl', 'url': 'https://www.dutchnews.nl/feed/'},
            {'name': 'NL Times', 'url': 'https://nltimes.nl/rss.xml'}
        ],
        'Russia': [
            {'name': 'RT News', 'url': 'https://www.rt.com/rss/'},
            {'name': 'TASS', 'url': 'https://tass.com/rss/v2.xml'}
        ],
        
        # Latin America
        'Brazil': [
            {'name': 'Brazil News', 'url': 'https://rss.cnn.com/rss/edition_americas.rss'},
            {'name': 'Rio Times', 'url': 'https://riotimesonline.com/feed/'}
        ],
        'Mexico': [
            {'name': 'Mexico News Daily', 'url': 'https://mexiconewsdaily.com/feed/'},
            {'name': 'El Universal English', 'url': 'https://www.eluniversal.com.mx/rss.xml'}
        ],
        'Argentina': [
            {'name': 'Buenos Aires Herald', 'url': 'https://www.buenosairesherald.com/rss.xml'},
            {'name': 'Argentina News', 'url': 'https://en.mercopress.com/rss/news'}
        ]
    }
    
    # Request headers to avoid blocking
    HEADERS = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'application/rss+xml, application/xml, text/xml',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1'
    }

class NewsRSSScraper:
    """Main scraper class for collecting news from RSS feeds"""
    
    def __init__(self, db_path: str = "news_database.db", rate_limit: float = 1.0):
        """
        Initialize the scraper
        
        Args:
            db_path: Path to SQLite database file
            rate_limit: Delay between requests in seconds
        """
        self.db_path = db_path
        self.rate_limit = rate_limit
        self.articles: List[NewsArticle] = []
        self.seen_hashes = set()
        self.config = GlobalNewsRSSConfig()
        self.session = requests.Session()
        self.session.headers.update(self.config.HEADERS)
        
        # Initialize database
        self._init_database()
    
    def _init_database(self):
        """Initialize SQLite database with required tables"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS news_articles (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    title TEXT NOT NULL,
                    publication_date TEXT,
                    source TEXT NOT NULL,
                    country TEXT NOT NULL,
                    summary TEXT,
                    url TEXT UNIQUE NOT NULL,
                    language TEXT,
                    category TEXT,
                    hash TEXT UNIQUE,
                    scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_country ON news_articles(country);
            ''')
            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_source ON news_articles(source);
            ''')
            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_date ON news_articles(publication_date);
            ''')
            conn.commit()
        logger.info("Database initialized successfully")
    
    def _detect_language(self, text: str) -> str:
        """Detect language of given text"""
        try:
            if text and len(text.strip()) > 10:
                return detect(text)
        except:
            pass
        return "unknown"
    
    def _parse_date(self, date_str: str) -> str:
        """Parse and standardize date format"""
        if not date_str:
            return datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        try:
            # Try parsing common RSS date formats
            formats = [
                '%a, %d %b %Y %H:%M:%S %z',
                '%a, %d %b %Y %H:%M:%S GMT',
                '%Y-%m-%dT%H:%M:%S%z',
                '%Y-%m-%d %H:%M:%S',
                '%d %b %Y %H:%M:%S',
            ]
            
            for fmt in formats:
                try:
                    dt = datetime.strptime(date_str.strip(), fmt)
                    return dt.strftime('%Y-%m-%d %H:%M:%S')
                except ValueError:
                    continue
        except Exception as e:
            logger.warning(f"Date parsing error for '{date_str}': {e}")
        
        return datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    def _clean_text(self, text: str) -> str:
        """Clean and normalize text content"""
        if not text:
            return ""
        
        # Remove HTML tags
        text = re.sub(r'<[^>]+>', '', text)
        
        # Normalize whitespace
        text = ' '.join(text.split())
        
        # Remove excessive punctuation
        text = re.sub(r'[.]{3,}', '...', text)
        
        return text.strip()
    
    def _fetch_rss_feed(self, url: str, timeout: int = 30) -> Optional[feedparser.FeedParserDict]:
        """
        Fetch and parse RSS feed from URL
        
        Args:
            url: RSS feed URL
            timeout: Request timeout in seconds
            
        Returns:
            Parsed feed data or None if failed
        """
        try:
            logger.info(f"Fetching RSS feed: {url}")
            
            # Add delay for rate limiting
            time.sleep(self.rate_limit)
            
            response = self.session.get(url, timeout=timeout)
            response.raise_for_status()
            
            # Parse feed
            feed = feedparser.parse(response.content)
            
            if feed.bozo:
                logger.warning(f"Feed parsing issues for {url}: {feed.bozo_exception}")
            
            return feed
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Network error fetching {url}: {e}")
        except Exception as e:
            logger.error(f"Unexpected error fetching {url}: {e}")
        
        return None
    
    def _extract_articles_from_feed(self, feed: feedparser.FeedParserDict, 
                                  source: str, country: str) -> List[NewsArticle]:
        """
        Extract articles from parsed RSS feed
        
        Args:
            feed: Parsed RSS feed data
            source: News source name
            country: Country name
            
        Returns:
            List of extracted NewsArticle objects
        """
        articles = []
        
        if not hasattr(feed, 'entries') or not feed.entries:
            logger.warning(f"No entries found in feed from {source}")
            return articles
        
        for entry in feed.entries:
            try:
                # Extract basic information
                title = self._clean_text(getattr(entry, 'title', ''))
                summary = self._clean_text(getattr(entry, 'summary', ''))
                url = getattr(entry, 'link', '')
                
                # Skip if essential fields are missing
                if not title or not url:
                    continue
                
                # Parse publication date
                pub_date = self._parse_date(getattr(entry, 'published', ''))
                
                # Create article object
                article = NewsArticle(
                    title=title,
                    publication_date=pub_date,
                    source=source,
                    country=country,
                    summary=summary,
                    url=url
                )
                
                # Detect language
                text_for_lang = f"{title} {summary}"
                article.language = self._detect_language(text_for_lang)
                
                # Check for duplicates
                article_hash = article.get_hash()
                if article_hash not in self.seen_hashes:
                    self.seen_hashes.add(article_hash)
                    articles.append(article)
                
            except Exception as e:
                logger.error(f"Error extracting article from {source}: {e}")
                continue
        
        logger.info(f"Extracted {len(articles)} articles from {source}")
        return articles
    
    def scrape_country_feeds(self, country: str) -> List[NewsArticle]:
        """
        Scrape all RSS feeds for a specific country
        
        Args:
            country: Country name
            
        Returns:
            List of scraped articles
        """
        country_articles = []
        
        if country not in self.config.RSS_FEEDS:
            logger.warning(f"No RSS feeds configured for {country}")
            return country_articles
        
        feeds = self.config.RSS_FEEDS[country]
        logger.info(f"Starting to scrape {len(feeds)} feeds for {country}")
        
        for feed_info in feeds:
            feed_name = feed_info['name']
            feed_url = feed_info['url']
            
            try:
                # Fetch and parse feed
                feed = self._fetch_rss_feed(feed_url)
                if not feed:
                    continue
                
                # Extract articles
                articles = self._extract_articles_from_feed(feed, feed_name, country)
                country_articles.extend(articles)
                
            except Exception as e:
                logger.error(f"Error processing feed {feed_name} ({country}): {e}")
                continue
        
        logger.info(f"Total articles scraped for {country}: {len(country_articles)}")
        return country_articles
    
    def scrape_all_countries(self, max_workers: int = 5) -> None:
        """
        Scrape RSS feeds from all configured countries using threading
        
        Args:
            max_workers: Maximum number of concurrent threads
        """
        logger.info("Starting to scrape all countries...")
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit tasks for each country
            future_to_country = {
                executor.submit(self.scrape_country_feeds, country): country 
                for country in self.config.RSS_FEEDS.keys()
            }
            
            # Collect results
            for future in as_completed(future_to_country):
                country = future_to_country[future]
                try:
                    articles = future.result()
                    self.articles.extend(articles)
                    logger.info(f"Completed scraping for {country}: {len(articles)} articles")
                except Exception as e:
                    logger.error(f"Error scraping {country}: {e}")
        
        logger.info(f"Scraping completed. Total articles: {len(self.articles)}")
    
    def save_to_database(self) -> None:
        """Save scraped articles to SQLite database"""
        if not self.articles:
            logger.warning("No articles to save")
            return
        
        saved_count = 0
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            for article in self.articles:
                try:
                    cursor.execute('''
                        INSERT OR IGNORE INTO news_articles 
                        (title, publication_date, source, country, summary, url, language, category, hash)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        article.title,
                        article.publication_date,
                        article.source,
                        article.country,
                        article.summary,
                        article.url,
                        article.language,
                        article.category,
                        article.get_hash()
                    ))
                    
                    if cursor.rowcount > 0:
                        saved_count += 1
                        
                except sqlite3.Error as e:
                    logger.error(f"Database error saving article: {e}")
            
            conn.commit()
        
        logger.info(f"Saved {saved_count} new articles to database")
    
    def export_to_csv(self, filename: str = "news_articles.csv") -> None:
        """Export articles to CSV file"""
        if not self.articles:
            logger.warning("No articles to export")
            return
        
        try:
            df = pd.DataFrame([article.to_dict() for article in self.articles])
            df.to_csv(filename, index=False, encoding='utf-8')
            logger.info(f"Exported {len(self.articles)} articles to {filename}")
        except Exception as e:
            logger.error(f"Error exporting to CSV: {e}")
    
    def export_to_json(self, filename: str = "news_articles.json") -> None:
        """Export articles to JSON file"""
        if not self.articles:
            logger.warning("No articles to export")
            return
        
        try:
            data = {
                'scraped_at': datetime.now().isoformat(),
                'total_articles': len(self.articles),
                'articles': [article.to_dict() for article in self.articles]
            }
            
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            
            logger.info(f"Exported {len(self.articles)} articles to {filename}")
        except Exception as e:
            logger.error(f"Error exporting to JSON: {e}")
    
    def get_statistics(self) -> Dict:
        """Generate statistics about scraped articles"""
        if not self.articles:
            return {}
        
        # Count by country
        country_counts = {}
        source_counts = {}
        language_counts = {}
        
        for article in self.articles:
            country_counts[article.country] = country_counts.get(article.country, 0) + 1
            source_counts[article.source] = source_counts.get(article.source, 0) + 1
            language_counts[article.language] = language_counts.get(article.language, 0) + 1
        
        return {
            'total_articles': len(self.articles),
            'countries': len(country_counts),
            'sources': len(source_counts),
            'articles_by_country': dict(sorted(country_counts.items(), key=lambda x: x[1], reverse=True)),
            'articles_by_source': dict(sorted(source_counts.items(), key=lambda x: x[1], reverse=True)),
            'articles_by_language': dict(sorted(language_counts.items(), key=lambda x: x[1], reverse=True))
        }
    
    def print_statistics(self) -> None:
        """Print comprehensive statistics"""
        stats = self.get_statistics()
        
        if not stats:
            print("No statistics available - no articles scraped")
            return
        
        print("\n" + "="*60)
        print("GLOBAL NEWS SCRAPING STATISTICS")
        print("="*60)
        print(f"Total Articles: {stats['total_articles']}")
        print(f"Countries Covered: {stats['countries']}")
        print(f"News Sources: {stats['sources']}")
        
        print("\nTop 10 Countries by Article Count:")
        print("-" * 40)
        for country, count in list(stats['articles_by_country'].items())[:10]:
            print(f"{country:<25} {count:>6}")
        
        print("\nTop 10 News Sources:")
        print("-" * 40)
        for source, count in list(stats['articles_by_source'].items())[:10]:
            print(f"{source:<25} {count:>6}")
        
        print("\nLanguage Distribution:")
        print("-" * 40)
        for language, count in stats['articles_by_language'].items():
            print(f"{language:<15} {count:>6}")

class NewsScheduler:
    """Scheduler for automated news scraping"""
    
    def __init__(self, scraper: NewsRSSScraper):
        self.scraper = scraper
        self.running = False
    
    def start_scheduled_scraping(self, interval_hours: int = 6):
        """Start scheduled scraping every N hours"""
        def run_scraper():
            logger.info("Starting scheduled news scraping...")
            self.scraper.scrape_all_countries()
            self.scraper.save_to_database()
            self.scraper.export_to_csv()
            logger.info("Scheduled scraping completed")
        
        # Schedule the job
        schedule.every(interval_hours).hours.do(run_scraper)
        
        self.running = True
        while self.running:
            schedule.run_pending()
            time.sleep(60)  # Check every minute
    
    def stop(self):
        """Stop the scheduler"""
        self.running = False

def main():
    """Main execution function"""
    print("Global News RSS Scraper")
    print("="*50)
    
    # Initialize scraper
    scraper = NewsRSSScraper(rate_limit=1.0)
    
    try:
        # Scrape all countries
        scraper.scrape_all_countries(max_workers=3)
        
        # Save to database
        scraper.save_to_database()
        
        # Export to files
        scraper.export_to_csv("global_news.csv")
        scraper.export_to_json("global_news.json")
        
        # Print statistics
        scraper.print_statistics()
        
        print("\nScraping completed successfully!")
        print(f"Data saved to: news_database.db")
        print(f"CSV export: global_news.csv")
        print(f"JSON export: global_news.json")
        
    except KeyboardInterrupt:
        logger.info("Scraping interrupted by user")
    except Exception as e:
        logger.error(f"Unexpected error in main execution: {e}")
        raise

if __name__ == "__main__":
    main()