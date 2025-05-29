
"""
Flask API for Global News RSS Scraper
=====================================

A RESTful API to serve scraped news data with filtering, pagination, and search capabilities.

Author: Global News API
Date: 2025
"""

from flask import Flask, request, jsonify, render_template_string
import sqlite3
from datetime import datetime, timedelta
import json
from typing import Dict, List, Optional, Tuple
import logging
from functools import wraps
import os

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.config['JSON_SORT_KEYS'] = False

# Database configuration
DATABASE_PATH = 'news_database.db'

class NewsAPI:
    """API class for handling news database operations"""
    
    def __init__(self, db_path: str = DATABASE_PATH):
        self.db_path = db_path
        self._ensure_database_exists()
    
    def _ensure_database_exists(self):
        """Ensure the database exists and is properly structured"""
        if not os.path.exists(self.db_path):
            logger.warning(f"Database {self.db_path} not found. Creating empty database.")
            self._create_empty_database()
    
    def _create_empty_database(self):
        """Create an empty database with proper schema"""
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
            conn.commit()
    
    def get_connection(self):
        """Get database connection with Row factory"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn
    
    def get_articles(self, 
                    country: Optional[str] = None,
                    source: Optional[str] = None,
                    language: Optional[str] = None,
                    date_from: Optional[str] = None,
                    date_to: Optional[str] = None,
                    search: Optional[str] = None,
                    limit: int = 100,
                    offset: int = 0,
                    sort_by: str = 'publication_date',
                    sort_order: str = 'DESC') -> Tuple[List[Dict], int]:
        """
        Get articles with filtering and pagination
        
        Returns:
            Tuple of (articles_list, total_count)
        """
        query_conditions = []
        query_params = []
        
        # Build WHERE clause
        if country:
            query_conditions.append("country = ?")
            query_params.append(country)
        
        if source:
            query_conditions.append("source = ?")
            query_params.append(source)
        
        if language:
            query_conditions.append("language = ?")
            query_params.append(language)
        
        if date_from:
            query_conditions.append("date(publication_date) >= date(?)")
            query_params.append(date_from)
        
        if date_to:
            query_conditions.append("date(publication_date) <= date(?)")
            query_params.append(date_to)
        
        if search:
            query_conditions.append("(title LIKE ? OR summary LIKE ?)")
            search_term = f"%{search}%"
            query_params.extend([search_term, search_term])
        
        # Build base query
        where_clause = " AND ".join(query_conditions) if query_conditions else "1=1"
        
        # Validate sort parameters
        valid_sort_columns = ['publication_date', 'title', 'source', 'country', 'scraped_at']
        if sort_by not in valid_sort_columns:
            sort_by = 'publication_date'
        
        if sort_order.upper() not in ['ASC', 'DESC']:
            sort_order = 'DESC'
        
        # Get total count
        count_query = f"SELECT COUNT(*) FROM news_articles WHERE {where_clause}"
        
        # Get articles with pagination
        articles_query = f"""
            SELECT id, title, publication_date, source, country, summary, url, language, category, scraped_at
            FROM news_articles 
            WHERE {where_clause}
            ORDER BY {sort_by} {sort_order}
            LIMIT ? OFFSET ?
        """
        
        with self.get_connection() as conn:
            # Get total count
            cursor = conn.execute(count_query, query_params)
            total_count = cursor.fetchone()[0]
            
            # Get articles
            cursor = conn.execute(articles_query, query_params + [limit, offset])
            articles = [dict(row) for row in cursor.fetchall()]
        
        return articles, total_count
    
    def get_article_by_id(self, article_id: int) -> Optional[Dict]:
        """Get a single article by ID"""
        query = "SELECT * FROM news_articles WHERE id = ?"
        
        with self.get_connection() as conn:
            cursor = conn.execute(query, (article_id,))
            row = cursor.fetchone()
            return dict(row) if row else None
    
    def get_statistics(self) -> Dict:
        """Get comprehensive statistics about the news database"""
        with self.get_connection() as conn:
            # Total articles
            cursor = conn.execute("SELECT COUNT(*) FROM news_articles")
            total_articles = cursor.fetchone()[0]
            
            # Articles by country
            cursor = conn.execute("""
                SELECT country, COUNT(*) as count 
                FROM news_articles 
                GROUP BY country 
                ORDER BY count DESC
            """)
            countries = [{'country': row[0], 'count': row[1]} for row in cursor.fetchall()]
            
            # Articles by source
            cursor = conn.execute("""
                SELECT source, COUNT(*) as count 
                FROM news_articles 
                GROUP BY source 
                ORDER BY count DESC
                LIMIT 20
            """)
            sources = [{'source': row[0], 'count': row[1]} for row in cursor.fetchall()]
            
            # Articles by language
            cursor = conn.execute("""
                SELECT language, COUNT(*) as count 
                FROM news_articles 
                GROUP BY language 
                ORDER BY count DESC
            """)
            languages = [{'language': row[0], 'count': row[1]} for row in cursor.fetchall()]
            
            # Recent activity (last 7 days)
            cursor = conn.execute("""
                SELECT DATE(publication_date) as date, COUNT(*) as count
                FROM news_articles 
                WHERE DATE(publication_date) >= DATE('now', '-7 days')
                GROUP BY DATE(publication_date)
                ORDER BY date DESC
            """)
            recent_activity = [{'date': row[0], 'count': row[1]} for row in cursor.fetchall()]
            
            # Latest scraping time
            cursor = conn.execute("SELECT MAX(scraped_at) FROM news_articles")
            latest_scrape = cursor.fetchone()[0]
        
        return {
            'total_articles': total_articles,
            'countries': countries,
            'sources': sources,
            'languages': languages,
            'recent_activity': recent_activity,
            'latest_scrape': latest_scrape,
            'unique_countries': len(countries),
            'unique_sources': len(sources)
        }
    
    def search_articles(self, query: str, limit: int = 50) -> List[Dict]:
        """Full-text search in articles"""
        search_query = """
            SELECT id, title, publication_date, source, country, summary, url, language,
                   CASE 
                       WHEN title LIKE ? THEN 3
                       WHEN summary LIKE ? THEN 2
                       ELSE 1
                   END as relevance_score
            FROM news_articles 
            WHERE title LIKE ? OR summary LIKE ?
            ORDER BY relevance_score DESC, publication_date DESC
            LIMIT ?
        """
        
        search_term = f"%{query}%"
        params = [search_term, search_term, search_term, search_term, limit]
        
        with self.get_connection() as conn:
            cursor = conn.execute(search_query, params)
            return [dict(row) for row in cursor.fetchall()]

# Initialize API
news_api = NewsAPI()

# Error handling decorator
def handle_errors(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        except Exception as e:
            logger.error(f"API Error: {str(e)}")
            return jsonify({
                'error': 'Internal server error',
                'message': str(e)
            }), 500
    return decorated_function

# Helper functions
def get_query_param(param_name: str, default_value=None, param_type=str):
    """Get query parameter with type conversion"""
    value = request.args.get(param_name, default_value)
    if value is None:
        return default_value
    
    try:
        if param_type == int:
            return int(value)
        elif param_type == bool:
            return value.lower() in ('true', '1', 'yes', 'on')
        else:
            return str(value)
    except (ValueError, TypeError):
        return default_value

def format_response(data, message: str = "Success", status_code: int = 200):
    """Format API response consistently"""
    response = {
        'status': 'success' if status_code < 400 else 'error',
        'message': message,
        'data': data,
        'timestamp': datetime.now().isoformat()
    }
    return jsonify(response), status_code

# API Routes

@app.route('/')
def index():
    """API documentation and welcome page"""
    html_template = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>Global News API</title>
        <style>
            body { font-family: Arial, sans-serif; margin: 40px; line-height: 1.6; }
            .endpoint { background: #f4f4f4; padding: 15px; margin: 10px 0; border-radius: 5px; }
            .method { color: #fff; padding: 5px 10px; border-radius: 3px; font-weight: bold; }
            .get { background: #61affe; }
            .post { background: #49cc90; }
            code { background: #f4f4f4; padding: 2px 4px; border-radius: 3px; }
        </style>
    </head>
    <body>
        <h1>🌍 Global News RSS Scraper API</h1>
        <p>RESTful API for accessing scraped news data from around the world.</p>
        
        <h2>📋 Available Endpoints</h2>
        
        <div class="endpoint">
            <span class="method get">GET</span> <strong>/api/articles</strong>
            <p>Get articles with filtering and pagination</p>
            <p><strong>Parameters:</strong> country, source, language, date_from, date_to, search, limit, offset, sort_by, sort_order</p>
        </div>
        
        <div class="endpoint">
            <span class="method get">GET</span> <strong>/api/articles/{id}</strong>
            <p>Get a specific article by ID</p>
        </div>
        
        <div class="endpoint">
            <span class="method get">GET</span> <strong>/api/search</strong>
            <p>Search articles by keyword</p>
            <p><strong>Parameters:</strong> q (query), limit</p>
        </div>
        
        <div class="endpoint">
            <span class="method get">GET</span> <strong>/api/statistics</strong>
            <p>Get comprehensive statistics about the news database</p>
        </div>
        
        <div class="endpoint">
            <span class="method get">GET</span> <strong>/api/countries</strong>
            <p>Get list of available countries</p>
        </div>
        
        <div class="endpoint">
            <span class="method get">GET</span> <strong>/api/sources</strong>
            <p>Get list of available news sources</p>
        </div>
        
        <h2>🔍 Example Requests</h2>
        <ul>
            <li><code>/api/articles?country=United%20States&limit=10</code></li>
            <li><code>/api/articles?source=BBC%20News&date_from=2025-05-01</code></li>
            <li><code>/api/search?q=technology&limit=20</code></li>
            <li><code>/api/statistics</code></li>
        </ul>
        
        <h2>📊 Response Format</h2>
        <pre>{
  "status": "success",
  "message": "Success", 
  "data": { ... },
  "timestamp": "2025-05-28T10:30:00"
}</pre>
        
        <p><strong>Built with:</strong> Flask, SQLite, Python</p>
    </body>
    </html>
    """
    return html_template

@app.route('/api/articles', methods=['GET'])
@handle_errors
def get_articles():
    """Get articles with filtering and pagination"""
    # Get query parameters
    country = get_query_param('country')
    source = get_query_param('source')
    language = get_query_param('language')
    date_from = get_query_param('date_from')
    date_to = get_query_param('date_to')
    search = get_query_param('search')
    limit = get_query_param('limit', 100, int)
    offset = get_query_param('offset', 0, int)
    sort_by = get_query_param('sort_by', 'publication_date')
    sort_order = get_query_param('sort_order', 'DESC')
    
    # Validate parameters
    limit = min(max(1, limit), 1000)  # Between 1 and 1000
    offset = max(0, offset)  # Non-negative
    
    # Get articles
    articles, total_count = news_api.get_articles(
        country=country,
        source=source,
        language=language,
        date_from=date_from,
        date_to=date_to,
        search=search,
        limit=limit,
        offset=offset,
        sort_by=sort_by,
        sort_order=sort_order
    )
    
    # Prepare response
    response_data = {
        'articles': articles,
        'pagination': {
            'total': total_count,
            'limit': limit,
            'offset': offset,
            'pages': (total_count + limit - 1) // limit,
            'current_page': (offset // limit) + 1
        },
        'filters': {
            'country': country,
            'source': source,
            'language': language,
            'date_from': date_from,
            'date_to': date_to,
            'search': search
        }
    }
    
    return format_response(response_data, f"Retrieved {len(articles)} articles")

@app.route('/api/articles/<int:article_id>', methods=['GET'])
@handle_errors
def get_article(article_id):
    """Get a specific article by ID"""
    article = news_api.get_article_by_id(article_id)
    
    if not article:
        return format_response(None, "Article not found", 404)
    
    return format_response(article, "Article retrieved successfully")

@app.route('/api/search', methods=['GET'])
@handle_errors
def search_articles():
    """Search articles by keyword"""
    query = get_query_param('q')
    limit = get_query_param('limit', 50, int)
    
    if not query:
        return format_response(None, "Query parameter 'q' is required", 400)
    
    limit = min(max(1, limit), 500)  # Between 1 and 500
    
    articles = news_api.search_articles(query, limit)
    
    response_data = {
        'query': query,
        'results': articles,
        'count': len(articles)
    }
    
    return format_response(response_data, f"Found {len(articles)} articles")

@app.route('/api/statistics', methods=['GET'])
@handle_errors
def get_statistics():
    """Get comprehensive statistics"""
    stats = news_api.get_statistics()
    return format_response(stats, "Statistics retrieved successfully")

@app.route('/api/countries', methods=['GET'])
@handle_errors
def get_countries():
    """Get list of available countries"""
    with news_api.get_connection() as conn:
        cursor = conn.execute("""
            SELECT country, COUNT(*) as article_count 
            FROM news_articles 
            GROUP BY country 
            ORDER BY article_count DESC
        """)
        countries = [{'country': row[0], 'article_count': row[1]} for row in cursor.fetchall()]
    
    return format_response(countries, f"Retrieved {len(countries)} countries")

@app.route('/api/sources', methods=['GET'])
@handle_errors
def get_sources():
    """Get list of available news sources"""
    country = get_query_param('country')
    
    query = """
        SELECT source, country, COUNT(*) as article_count 
        FROM news_articles 
    """
    params = []
    
    if country:
        query += " WHERE country = ?"
        params.append(country)
    
    query += " GROUP BY source, country ORDER BY article_count DESC"
    
    with news_api.get_connection() as conn:
        cursor = conn.execute(query, params)
        sources = [{'source': row[0], 'country': row[1], 'article_count': row[2]} 
                  for row in cursor.fetchall()]
    
    return format_response(sources, f"Retrieved {len(sources)} sources")

@app.route('/api/health', methods=['GET'])
@handle_errors
def health_check():
    """Health check endpoint"""
    try:
        # Test database connection
        with news_api.get_connection() as conn:
            cursor = conn.execute("SELECT COUNT(*) FROM news_articles")
            article_count = cursor.fetchone()[0]
        
        health_data = {
            'status': 'healthy',
            'database': 'connected',
            'total_articles': article_count,
            'timestamp': datetime.now().isoformat()
        }
        
        return format_response(health_data, "Service is healthy")
    
    except Exception as e:
        return format_response({
            'status': 'unhealthy',
            'error': str(e)
        }, "Service health check failed", 500)

# Error handlers
@app.errorhandler(404)
def not_found(error):
    return format_response(None, "Endpoint not found", 404)

@app.errorhandler(405)
def method_not_allowed(error):
    return format_response(None, "Method not allowed", 405)

@app.errorhandler(500)
def internal_error(error):
    return format_response(None, "Internal server error", 500)

# Development configuration
if __name__ == '__main__':
    print("🚀 Starting Global News API Server...")
    print("📖 API Documentation: http://localhost:5000/")
    print("🔍 Example endpoint: http://localhost:5000/api/articles")
    print("📊 Statistics: http://localhost:5000/api/statistics")
    
    app.run(debug=True, host='0.0.0.0', port=5000)