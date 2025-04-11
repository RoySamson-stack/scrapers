import tweepy
import pandas as pd
import time
import datetime
import json
import argparse
from typing import List, Dict, Any, Optional

class TwitterScraper:
    def __init__(self, bearer_token: str):
        """Initialize the Twitter scraper with authentication credentials.
        
        Args:
            bearer_token: Twitter API Bearer Token
        """
        self.client = tweepy.Client(bearer_token=bearer_token)
        
    def search_tweets(self, query: str, max_results: int = 100, 
                     start_time: Optional[datetime.datetime] = None,
                     end_time: Optional[datetime.datetime] = None) -> List[Dict[Any, Any]]:
        """Search for tweets matching a query.
        
        Args:
            query: The search query
            max_results: Maximum number of results to return (10-100)
            start_time: Optional start time for tweets
            end_time: Optional end time for tweets
            
        Returns:
            List of tweet data dictionaries
        """
        tweets = []
        
        # Twitter API v2 tweet fields we want to retrieve
        tweet_fields = ['id', 'text', 'author_id', 'created_at', 'public_metrics', 'lang']
        user_fields = ['id', 'name', 'username', 'location', 'verified']
        expansions = ['author_id']
        
        # Limit single request to max 100 (API limit)
        request_size = min(max_results, 100)
        
        # Handle rate limiting with exponential backoff
        max_retries = 5
        retry_count = 0
        retry_delay = 60  # Start with 60 seconds
        
        while retry_count <= max_retries:
            try:
                # Make the API request
                response = self.client.search_recent_tweets(
                    query=query,
                    max_results=request_size,
                    tweet_fields=tweet_fields,
                    user_fields=user_fields,
                    expansions=expansions,
                    start_time=start_time,
                    end_time=end_time
                )
                
                if not response.data:
                    print("No tweets found matching the query.")
                    break
                    
                # Process users to create a lookup dictionary
                users = {user.id: user for user in response.includes['users']} if 'users' in response.includes else {}
                
                # Process each tweet
                for tweet in response.data:
                    tweet_data = {
                        'id': tweet.id,
                        'text': tweet.text,
                        'created_at': tweet.created_at,
                        'retweet_count': tweet.public_metrics['retweet_count'],
                        'reply_count': tweet.public_metrics['reply_count'],
                        'like_count': tweet.public_metrics['like_count'],
                        'quote_count': tweet.public_metrics['quote_count'],
                        'lang': tweet.lang
                    }
                    
                    # Add author information if available
                    if tweet.author_id in users:
                        user = users[tweet.author_id]
                        tweet_data['author_id'] = user.id
                        tweet_data['author_name'] = user.name
                        tweet_data['author_username'] = user.username
                        tweet_data['author_location'] = user.location
                        tweet_data['author_verified'] = user.verified
                    
                    tweets.append(tweet_data)
                
                # Successfully retrieved tweets, break out of retry loop
                break
                    
            except tweepy.TooManyRequests:
                if retry_count < max_retries:
                    print(f"Rate limit exceeded. Waiting {retry_delay} seconds before retrying...")
                    time.sleep(retry_delay)
                    retry_delay *= 2  # Exponential backoff
                    retry_count += 1
                else:
                    print("Maximum retry attempts reached. Could not complete the request due to rate limiting.")
                    break
                    
            except tweepy.TweepyException as e:
                print(f"Error: {e}")
                break
                
        return tweets
    
    def save_to_csv(self, tweets: List[Dict[Any, Any]], filename: str) -> None:
        """Save tweets to a CSV file.
        
        Args:
            tweets: List of tweet data dictionaries
            filename: Output filename
        """
        if not tweets:
            print(f"No tweets to save to {filename}")
            return
            
        df = pd.DataFrame(tweets)
        df.to_csv(filename, index=False)
        print(f"Saved {len(tweets)} tweets to {filename}")
        
    def save_to_json(self, tweets: List[Dict[Any, Any]], filename: str) -> None:
        """Save tweets to a JSON file.
        
        Args:
            tweets: List of tweet data dictionaries
            filename: Output filename
        """
        if not tweets:
            print(f"No tweets to save to {filename}")
            return
            
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(tweets, f, ensure_ascii=False, indent=4, default=str)
        print(f"Saved {len(tweets)} tweets to {filename}")

def main():
    parser = argparse.ArgumentParser(description='Twitter Scraper')
    parser.add_argument('--token', '-t', type=str, required=True, help='Twitter API Bearer Token')
    parser.add_argument('--query', '-q', type=str, required=True, help='Search query')
    parser.add_argument('--count', '-c', type=int, default=100, help='Number of tweets to retrieve (max 100)')
    parser.add_argument('--output', '-o', type=str, default='tweets.csv', help='Output file')
    parser.add_argument('--format', '-f', type=str, choices=['csv', 'json'], default='csv', help='Output format')
    parser.add_argument('--days', '-d', type=int, default=7, help='Number of days back to search (max 7)')
    
    args = parser.parse_args()
    
    # Ensure count doesn't exceed 100
    if args.count > 100:
        print("Warning: API can only return max 100 tweets per request. Setting count to 100.")
        args.count = 100
    
    # Calculate start time based on days argument (limit to 7 days per Twitter API rules)
    days_back = min(args.days, 7)
    end_time = datetime.datetime.now(datetime.timezone.utc)
    start_time = end_time - datetime.timedelta(days=days_back)
    
    # Initialize scraper
    scraper = TwitterScraper(args.token)
    
    # Search for tweets
    print(f"Searching for tweets matching '{args.query}'...")
    tweets = scraper.search_tweets(
        query=args.query,
        max_results=args.count,
        start_time=start_time,
        end_time=end_time
    )
    
    print(f"Retrieved {len(tweets)} tweets")
    
    # Save results
    if args.format == 'csv':
        scraper.save_to_csv(tweets, args.output)
    else:
        scraper.save_to_json(tweets, args.output)

if __name__ == "__main__":
    main()