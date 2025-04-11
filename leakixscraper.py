import requests
from bs4 import BeautifulSoup
import pandas as pd
import json
import time
import random
import argparse
from urllib.parse import urljoin
from typing import List, Dict, Any, Optional

class WebsiteScraper:
    def __init__(self, base_url: str, headers: Optional[Dict[str, str]] = None):
        """Initialize the website scraper.
        
        Args:
            base_url: The base URL of the website to scrape
            headers: Optional HTTP headers for requests
        """
        self.base_url = base_url
        self.headers = headers or {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        }
        self.session = requests.Session()
        
    def get_page(self, url: str, retry_count: int = 3) -> Optional[BeautifulSoup]:
        """Get and parse a page.
        
        Args:
            url: The URL to fetch
            retry_count: Number of retries on failure
            
        Returns:
            BeautifulSoup object of the parsed page, or None on failure
        """
        full_url = urljoin(self.base_url, url)
        
        for attempt in range(retry_count):
            try:
                print(f"Fetching {full_url}...")
                response = self.session.get(full_url, headers=self.headers, timeout=10)
                
                if response.status_code == 200:
                    return BeautifulSoup(response.text, 'html.parser')
                else:
                    print(f"Failed to fetch {full_url}, status code: {response.status_code}")
                    
            except requests.RequestException as e:
                print(f"Error fetching {full_url}: {e}")
                
            # Wait before retrying with exponential backoff
            if attempt < retry_count - 1:
                sleep_time = 2 ** attempt + random.uniform(0, 1)
                print(f"Retrying in {sleep_time:.2f} seconds...")
                time.sleep(sleep_time)
                
        return None
        
    def scrape_leakix_homepage(self) -> List[Dict[str, Any]]:
        """Scrape the LeakIX homepage for leaked services.
        
        Returns:
            List of dictionaries containing information about leaked services
        """
        soup = self.get_page("/")
        if not soup:
            return []
            
        results = []
        
        # Example: Find the service cards on LeakIX homepage
        service_cards = soup.select('div.card')  # Adjust selector based on actual HTML structure
        
        for card in service_cards:
            try:
                # These selectors are examples - you'll need to adjust based on actual HTML
                service_data = {
                    'title': card.select_one('h3.card-title').text.strip() if card.select_one('h3.card-title') else "N/A",
                    'description': card.select_one('p.card-text').text.strip() if card.select_one('p.card-text') else "N/A",
                    'url': urljoin(self.base_url, card.select_one('a')['href']) if card.select_one('a') else None,
                    'timestamp': card.select_one('span.date').text.strip() if card.select_one('span.date') else "N/A",
                }
                
                results.append(service_data)
                
            except Exception as e:
                print(f"Error parsing card: {e}")
                
        return results
        
    def scrape_search_results(self, query: str, pages: int = 1) -> List[Dict[str, Any]]:
        """Scrape search results for a given query.
        
        Args:
            query: Search query
            pages: Number of pages to scrape
            
        Returns:
            List of dictionaries containing search results
        """
        all_results = []
        
        for page in range(1, pages + 1):
            url = f"/search?q={query}&page={page}"
            soup = self.get_page(url)
            
            if not soup:
                break
                
            # Example: Extract search results from the page
            result_items = soup.select('div.search-result')  # Adjust selector based on actual HTML
            
            if not result_items:
                print(f"No results found on page {page}")
                break
                
            for item in result_items:
                try:
                    # These selectors are examples - you'll need to adjust based on actual HTML
                    result_data = {
                        'title': item.select_one('h4.result-title').text.strip() if item.select_one('h4.result-title') else "N/A",
                        'description': item.select_one('div.result-description').text.strip() if item.select_one('div.result-description') else "N/A",
                        'url': urljoin(self.base_url, item.select_one('a')['href']) if item.select_one('a') else None,
                        'details': self._extract_details(item),
                    }
                    
                    all_results.append(result_data)
                    
                except Exception as e:
                    print(f"Error parsing result item: {e}")
            
            # Be respectful of the website and avoid getting blocked
            if page < pages:
                sleep_time = random.uniform(2, 5)
                print(f"Waiting {sleep_time:.2f} seconds before fetching next page...")
                time.sleep(sleep_time)
        
        return all_results
    
    def _extract_details(self, element: BeautifulSoup) -> Dict[str, str]:
        """Extract detailed information from a result element.
        
        Args:
            element: BeautifulSoup element containing details
            
        Returns:
            Dictionary of detail key-value pairs
        """
        details = {}
        
        # Example: Extract key-value pairs from result details
        detail_elements = element.select('div.detail-item')  # Adjust selector based on actual HTML
        
        for detail in detail_elements:
            try:
                key_elem = detail.select_one('span.detail-key')
                value_elem = detail.select_one('span.detail-value')
                
                if key_elem and value_elem:
                    key = key_elem.text.strip()
                    value = value_elem.text.strip()
                    details[key] = value
                    
            except Exception as e:
                print(f"Error extracting detail: {e}")
                
        return details
    
    def scrape_service_details(self, service_url: str) -> Dict[str, Any]:
        """Scrape detailed information about a specific service.
        
        Args:
            service_url: URL of the service details page
            
        Returns:
            Dictionary containing service details
        """
        soup = self.get_page(service_url)
        if not soup:
            return {}
            
        # Example: Extract service details
        details = {
            'url': service_url,
            'title': soup.select_one('h1.service-title').text.strip() if soup.select_one('h1.service-title') else "N/A",
        }
        
        # Example: Extract additional information
        info_table = soup.select_one('table.info-table')
        if info_table:
            rows = info_table.select('tr')
            for row in rows:
                cells = row.select('td')
                if len(cells) >= 2:
                    key = cells[0].text.strip()
                    value = cells[1].text.strip()
                    details[key] = value
        
        return details
    
    def save_to_csv(self, data: List[Dict[Any, Any]], filename: str) -> None:
        """Save data to a CSV file.
        
        Args:
            data: List of dictionaries containing scraped data
            filename: Output filename
        """
        if not data:
            print(f"No data to save to {filename}")
            return
            
        df = pd.DataFrame(data)
        df.to_csv(filename, index=False)
        print(f"Saved {len(data)} items to {filename}")
        
    def save_to_json(self, data: List[Dict[Any, Any]], filename: str) -> None:
        """Save data to a JSON file.
        
        Args:
            data: List of dictionaries containing scraped data
            filename: Output filename
        """
        if not data:
            print(f"No data to save to {filename}")
            return
            
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        print(f"Saved {len(data)} items to {filename}")

def main():
    parser = argparse.ArgumentParser(description='Website Scraper for LeakIX.net')
    parser.add_argument('--mode', '-m', type=str, choices=['home', 'search', 'details'], default='home',
                        help='Scraping mode: home (homepage), search (search results), details (service details)')
    parser.add_argument('--query', '-q', type=str, help='Search query (for search mode)')
    parser.add_argument('--url', '-u', type=str, help='Service URL (for details mode)')
    parser.add_argument('--pages', '-p', type=int, default=1, help='Number of pages to scrape (for search mode)')
    parser.add_argument('--output', '-o', type=str, default='scraped_data.json', help='Output file')
    parser.add_argument('--format', '-f', type=str, choices=['csv', 'json'], default='json', help='Output format')
    
    args = parser.parse_args()
    
    # Initialize scraper
    scraper = WebsiteScraper("https://leakix.net")
    
    # Scrape based on mode
    if args.mode == 'home':
        print("Scraping LeakIX homepage...")
        data = scraper.scrape_leakix_homepage()
    elif args.mode == 'search':
        if not args.query:
            print("Error: Search query required for search mode")
            return
        print(f"Searching LeakIX for '{args.query}' across {args.pages} page(s)...")
        data = scraper.scrape_search_results(args.query, args.pages)
    elif args.mode == 'details':
        if not args.url:
            print("Error: Service URL required for details mode")
            return
        print(f"Scraping details for {args.url}...")
        data = [scraper.scrape_service_details(args.url)]
    else:
        print(f"Unknown mode: {args.mode}")
        return
    
    # Save results
    if args.format == 'csv':
        scraper.save_to_csv(data, args.output)
    else:
        scraper.save_to_json(data, args.output)

if __name__ == "__main__":
    main()