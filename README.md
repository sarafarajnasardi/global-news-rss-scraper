# 🌍 Global News RSS Scraper

A comprehensive Python-based web scraping + API solution for extracting and serving news articles from RSS feeds across 20+ countries and 60+ news agencies worldwide.

---

## 🚀 Features

- **Multi-Country Coverage**: Scrapes news from 20+ countries including US, UK, Japan, India, China, Germany, France, and more
- **Multiple News Sources**: Over 60 RSS feeds from major global agencies
- **Comprehensive Data Extraction**: Title, date, source, country, summary, URL, language
- **Language Detection**: Automatically tags articles with language using `langdetect`
- **Duplicate Detection**: Advanced hash-based deduplication
- **Multi-Format Export**: Save as CSV, JSON, or into a SQLite database
- **API Interface**: Flask-based REST API for querying news
- **Full-Text Search**: Search titles and summaries
- **Concurrent Processing**: Multi-threaded scraping for performance
- **Rate Limiting**: Respectful scraping with configurable delay
- **Error Handling & Logging**: Built-in logging and resilience
- **Scheduling**: Run scraper automatically using `schedule`
- **Historical Archiving**: Supports continuous historical collection

---

## 📡 Covered Countries & Sources

### 🌐 English-Speaking Countries
- **UK**: BBC News, The Guardian, Reuters UK, Sky News
- **USA**: CNN, Reuters US, AP, NPR
- **Canada**: CBC, CTV News
- **Australia**: ABC News, The Australian

### 🌏 Asia
- **India**: Times of India, The Hindu, NDTV
- **Japan**: NHK, Japan Times
- **China**: China Daily, Global Times
- **Singapore**: CNA, Straits Times
- **Malaysia**: The Star, NST
- **Indonesia**: Jakarta Post, Antara News
- **South Korea**: Yonhap, Korea Herald

### 🌍 Middle East & Africa
- **Middle East**: Al Jazeera, Al Arabiya
- **Israel**: Haaretz, Jerusalem Post
- **South Africa**: News24, IOL

### 🇪🇺 Europe
- **Germany**: DW, The Local DE
- **France**: France 24, The Local FR
- **Italy**: ANSA, The Local IT
- **Spain**: El País EN, The Local ES
- **Netherlands**: DutchNews.nl, NL Times
- **Russia**: RT, TASS

### 🌎 Latin America
- **Brazil**: Rio Times, Agência Brasil
- **Mexico**: El Universal, Mexico News Daily
- **Argentina**: Buenos Aires Herald

---

## 🧰 Installation

### Prerequisites
- Python 3.7+
- pip

### Install Dependencies
```bash
pip install -r requirements.txt
```

### `requirements.txt`
```
feedparser>=6.0.0
requests>=2.25.0
pandas>=1.3.0
beautifulsoup4>=4.9.0
langdetect>=1.0.9
schedule>=1.1.0
flask>=2.0.0
```

---

## 📖 Usage

### 🐍 Python (Scripted)
```python
from news_scraper import NewsRSSScraper

scraper = NewsRSSScraper()
scraper.scrape_all_countries()
scraper.save_to_database()
scraper.export_to_csv("news_data.csv")
scraper.export_to_json("news_data.json")
scraper.print_statistics()
```

### 🛠 Advanced Usage

#### Scrape Specific Countries
```python
countries = ['United States', 'United Kingdom', 'Japan']
for country in countries:
    articles = scraper.scrape_country_feeds(country)
    scraper.articles.extend(articles)
```

#### Add Rate Limiting
```python
scraper = NewsRSSScraper(rate_limit_seconds=2)
```

---

## 🌐 API Interface (Flask)

### Start the API:
```bash
python api.py
```

### Example Endpoints:
- `GET /api/articles` – with filters: `?country=India&limit=10`
- `GET /api/articles/<id>` – get article by ID
- `GET /api/search?q=election` – full-text search
- `GET /api/statistics` – analytics summary
- `GET /api/countries` – list countries with article counts
- `GET /api/sources` – list sources

---

## ⏱ Scheduling

Use `scheduler.py` to automatically scrape every hour:
```bash
python scheduler.py
```

---

## 📦 Output Files

- `news_data.csv` – Full export
- `news_data.json` – JSON format
- `news_data.db` – SQLite database

---

## 📊 Example Article Schema

```json
{
  "title": "India's space mission update",
  "publication_date": "2025-05-27T14:30:00Z",
  "source": "The Hindu",
  "country": "India",
  "summary": "India launched...",
  "url": "https://...",
  "language": "en",
  "scraped_at": "2025-05-28T10:00:00"
}
```

---

## 🧠 Bonus / Advanced Features

The following advanced features were implemented as part of this project:

- ✅ **SQLite Database Integration**: All scraped articles are stored in a structured SQLite database (`news_data.db`), enabling fast queries and long-term persistence.
- ✅ **RESTful Flask API**: A fully featured Flask-based API is provided to query and serve data through endpoints including:
  - `GET /api/articles`: Retrieve articles with filters for country, source, date, language, and keyword search.
  - `GET /api/articles/<id>`: Get a single article by ID.
  - `GET /api/search?q=...`: Full-text search in article titles and summaries.
  - `GET /api/statistics`: Summary stats on countries, languages, and article counts.
  - `GET /api/countries`: List all countries represented in the data.
  - `GET /api/sources`: List all news sources.
- ✅ **Search & Ranking**: Articles can be searched and ranked by relevance.
- ✅ **Auto-Creation of Database Schema**: Ensures the schema exists if running for the first time.
- ✅ **Filter & Pagination Support**: Full support for `limit`, `offset`, and custom sorting in API results.

---
