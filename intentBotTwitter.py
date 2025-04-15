import pandas as pd
import time
import pytz
import os
from datetime import datetime
import undetected_chromedriver as uc
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from cosine_sim import calculate_similarity_scores  # Ensure cosine_sim.py exists

# New imports for Google Sheets API
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from gspread_dataframe import set_with_dataframe, get_as_dataframe

# Import for tracking execution time
import time as time_module
from datetime import timedelta

# Predefined intent sentences for comparison
intent_data = [
    "What are the latest AI breakthroughs?",
    "How can AI improve productivity?",
    "Will AI replace human jobs?",
    "What are the ethical concerns of AI?",
    "What is the best AI model for my use case?",
    "How can AI help small businesses grow?",
    "Which AI tools are worth using in 2025?",
    "Best AI research papers to read this year?",
    "How can I start learning AI development?",
    "What's the future of AI in creative industries?",
]

def setup_google_sheets():
    """Setup authentication with Google Sheets API."""
    print("Setting up Google Sheets connection...")
    # Define the scope
    scope = ['https://spreadsheets.google.com/feeds',
             'https://www.googleapis.com/auth/drive']
    
    # Add your credentials file path
    creds = ServiceAccountCredentials.from_json_keyfile_name('gspread-credentials.json', scope)
    client = gspread.authorize(creds)
    
    return client

def append_to_sheets(df, sheet_name, tab_name):
    """Append dataframe to Google Sheet, preserving existing data."""
    try:
        # Setup Google Sheets client
        client = setup_google_sheets()
        
        # Try to open existing spreadsheet or create new one
        try:
            sheet = client.open(sheet_name)
        except gspread.exceptions.SpreadsheetNotFound:
            sheet = client.create(sheet_name)
            # Make the spreadsheet publicly readable
            sheet.share(None, perm_type='anyone', role='reader')
        
        # Check if worksheet exists, create if not
        try:
            worksheet = sheet.worksheet(tab_name)
            # Get existing data
            existing_df = get_as_dataframe(worksheet, evaluate_formulas=True)
            # Remove empty rows
            existing_df = existing_df.dropna(how='all')
            
            if not existing_df.empty:
                print(f"Found existing data in sheet: {len(existing_df)} rows")
                # Concatenate with new data
                combined_df = pd.concat([existing_df, df], ignore_index=True)
                # Remove duplicates based on URL (DocURL or ReplyURL)
                if 'DocURL' in combined_df.columns:
                    combined_df = combined_df.drop_duplicates(subset=['DocURL'], keep='last')
                elif 'ReplyURL' in combined_df.columns:
                    combined_df = combined_df.drop_duplicates(subset=['ReplyURL'], keep='last')
            else:
                combined_df = df
                
        except gspread.exceptions.WorksheetNotFound:
            worksheet = sheet.add_worksheet(title=tab_name, rows="1000", cols="20")
            combined_df = df
        
        # Clear existing worksheet and upload combined data
        worksheet.clear()
        set_with_dataframe(worksheet, combined_df)
        
        sheet_url = f"https://docs.google.com/spreadsheets/d/{sheet.id}"
        print(f"Data appended successfully to Google Sheets: {sheet_url}")
        print(f"Total rows in sheet now: {len(combined_df)}")
        
        return sheet_url, combined_df
    
    except Exception as e:
        print(f"Error working with Google Sheets: {str(e)}")
        return None, df

def append_to_csv(df, filename):
    """Append dataframe to CSV file, preserving existing data."""
    combined_df = df
    
    try:
        if os.path.exists(filename):
            # Read existing data
            existing_df = pd.read_csv(filename)
            print(f"Found existing data in file: {len(existing_df)} rows")
            
            # Concatenate with new data
            combined_df = pd.concat([existing_df, df], ignore_index=True)
            
            # Remove duplicates based on URL (DocURL or ReplyURL)
            if 'DocURL' in combined_df.columns:
                combined_df = combined_df.drop_duplicates(subset=['DocURL'], keep='last')
            elif 'ReplyURL' in combined_df.columns:
                combined_df = combined_df.drop_duplicates(subset=['ReplyURL'], keep='last')
        
        # Save combined data
        combined_df.to_csv(filename, index=False)
        print(f"Data appended to {filename}")
        print(f"Total rows in file now: {len(combined_df)}")
        
    except Exception as e:
        print(f"Error appending to CSV: {str(e)}")
        # Attempt to save just the new data if there was an error
        try:
            df.to_csv(filename, index=False)
            print(f"Saved new data only to {filename}")
        except:
            print(f"Failed to save data to {filename}")
    
    return combined_df

def setup_driver():
    print("Setting up Chrome driver...")
    chrome_options = uc.ChromeOptions()

    chrome_options.add_argument(r'--user-data-dir=C:\Users\Shubham Dutta\AppData\Local\Google\Chrome\User Data')
    chrome_options.add_argument(r'--profile-directory=Profile 2')

    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")

    driver = uc.Chrome(options=chrome_options)
    return driver

def convert_to_ist(utc_datetime_str):
    """Converts UTC datetime string to IST timezone."""
    utc_datetime = datetime.fromisoformat(utc_datetime_str.replace("Z", "+00:00"))
    ist_timezone = pytz.timezone("Asia/Kolkata")
    ist_datetime = utc_datetime.astimezone(ist_timezone)
    return ist_datetime.strftime("%Y-%m-%d"), ist_datetime.strftime("%H:%M:%S")

def get_existing_urls(tweets_filename, replies_filename):
    """Get sets of existing URLs to avoid duplicates when scraping."""
    existing_tweet_urls = set()
    existing_reply_urls = set()
    
    try:
        if os.path.exists(tweets_filename):
            tweets_df = pd.read_csv(tweets_filename)
            if 'DocURL' in tweets_df.columns:
                existing_tweet_urls = set(tweets_df['DocURL'].dropna().tolist())
                print(f"Loaded {len(existing_tweet_urls)} existing tweet URLs")
    except Exception as e:
        print(f"Error loading existing tweets: {str(e)}")
    
    try:
        if os.path.exists(replies_filename):
            replies_df = pd.read_csv(replies_filename)
            if 'ReplyURL' in replies_df.columns:
                existing_reply_urls = set(replies_df['ReplyURL'].dropna().tolist())
                print(f"Loaded {len(existing_reply_urls)} existing reply URLs")
    except Exception as e:
        print(f"Error loading existing replies: {str(e)}")
    
    return existing_tweet_urls, existing_reply_urls

def scrape_tweet_replies(tweet_url, existing_reply_urls=None, max_replies=50):
    """Scrape replies for a specific tweet, skipping already seen URLs."""
    if existing_reply_urls is None:
        existing_reply_urls = set()
        
    driver = setup_driver()
    print(f"Opening tweet URL: {tweet_url}")
    driver.get(tweet_url)
    time.sleep(5)
    
    replies_data = []
    seen_reply_urls = set()
    scroll_count = 0
    max_scrolls = 30  # Limit scrolling to avoid infinite loops
    no_new_replies_count = 0
    
    try:
        # First, make sure the tweet loads
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.XPATH, '//article[@data-testid="tweet"]'))
        )
        
        # Wait a bit more to ensure replies have a chance to load
        time.sleep(3)
        
        # Try different approaches to find replies
        reply_containers_xpath = '//div[@aria-label="Timeline: Conversation" or @data-testid="reply"]//article'
        
        print("Waiting for replies to load...")
        
        while len(replies_data) < max_replies and scroll_count < max_scrolls and no_new_replies_count < 5:
            # Find all reply elements (excluding the original tweet)
            reply_elements = driver.find_elements(By.XPATH, reply_containers_xpath)
            
            # Debug output
            print(f"Found {len(reply_elements)} potential reply elements on screen")
            
            if len(reply_elements) <= 1:  # Only original tweet or no replies
                print("No replies found yet. Scrolling to load more content...")
                driver.execute_script("window.scrollBy(0, 800)")
                time.sleep(3)
                scroll_count += 1
                continue
                
            new_replies_found = False
            
            for reply in reply_elements:
                try:
                    # Skip elements that might be the original tweet
                    if scroll_count == 0 and reply_elements.index(reply) == 0:
                        continue
                        
                    # Try to get reply URL first
                    links = reply.find_elements(By.XPATH, './/a[contains(@href, "/status/")]')
                    if not links:
                        continue
                        
                    reply_url = links[0].get_attribute('href')
                    
                    # Skip if we've already seen this reply in this session or in previous runs
                    if reply_url in seen_reply_urls or reply_url in existing_reply_urls:
                        continue
                    
                    # Skip if this is actually the original tweet URL
                    if reply_url == tweet_url:
                        continue
                        
                    # Extract reply data
                    reply_info = {}
                    seen_reply_urls.add(reply_url)
                    reply_info["ReplyURL"] = reply_url
                    new_replies_found = True
                    
                    # Try to get profile info
                    try:
                        profile_element = reply.find_element(By.XPATH, './/div[@data-testid="User-Name"]')
                        profile_links = profile_element.find_elements(By.XPATH, './/a')
                        if len(profile_links) >= 2:
                            reply_info["Profile Link"] = profile_links[1].get_attribute('href')
                            reply_info["Profile Handle"] = profile_links[1].text
                        else:
                            reply_info["Profile Link"] = profile_links[0].get_attribute('href')
                            reply_info["Profile Handle"] = profile_links[0].text
                    except Exception as e:
                        print(f"Error getting profile info: {str(e)}")
                        reply_info["Profile Link"] = ""
                        reply_info["Profile Handle"] = ""
                    
                    # Try to get reply text
                    try:
                        tweet_text_element = reply.find_element(By.XPATH, './/div[@data-testid="tweetText"]')
                        reply_info["Reply Text"] = tweet_text_element.text
                    except Exception as e:
                        print(f"Error getting reply text: {str(e)}")
                        # Try an alternative approach
                        try:
                            reply_info["Reply Text"] = reply.text.split('\n')[2]  # Often the text is in the third line
                        except:
                            reply_info["Reply Text"] = "[Text extraction failed]"
                    
                    # Try to get time
                    try:
                        time_element = reply.find_element(By.XPATH, './/time')
                        utc_datetime_str = time_element.get_attribute('datetime')
                        reply_info["Date"], reply_info["Time"] = convert_to_ist(utc_datetime_str)
                    except Exception as e:
                        print(f"Error getting time: {str(e)}")
                        reply_info["Date"] = ""
                        reply_info["Time"] = ""
                    
                    reply_info["Original Tweet URL"] = tweet_url
                    replies_data.append(reply_info)
                    
                    # Print progress
                    if len(replies_data) % 5 == 0:
                        print(f"Found {len(replies_data)} new replies so far")
                        
                except Exception as e:
                    print(f"Error processing a reply: {str(e)}")
                    continue
            
            # Scroll down to load more replies
            driver.execute_script("window.scrollBy(0, 1000)")
            time.sleep(3)
            scroll_count += 1
            
            # Check if we found new replies in this scroll
            if not new_replies_found:
                no_new_replies_count += 1
                print(f"No new replies found in scroll #{scroll_count}. Attempt {no_new_replies_count}/5")
            else:
                no_new_replies_count = 0
                
            # Print scroll status
            print(f"Scrolled {scroll_count} times, found {len(replies_data)} replies so far")
                
    except Exception as e:
        print(f"Error during reply scraping: {str(e)}")
    
    # Extra validation before returning
    valid_replies = []
    for reply in replies_data:
        if "Reply Text" in reply and reply["Reply Text"] and "ReplyURL" in reply:
            valid_replies.append(reply)
    
    print(f"Found {len(valid_replies)} valid replies out of {len(replies_data)} total")
    driver.quit()
    return valid_replies


    
def scrape_tweets_with_metadata(keyword, existing_urls=None, max_tweets=1000, max_time_minutes=30):
    """
    Scrape tweets with infinite scrolling capability, skipping already seen URLs.
    
    Args:
        keyword: Search term to use
        existing_urls: Set of URLs that have already been scraped
        max_tweets: Maximum number of new tweets to collect
        max_time_minutes: Maximum time to run the scraper in minutes
        
    Returns:
        List of tweet data dictionaries
    """
    if existing_urls is None:
        existing_urls = set()
        
    driver = setup_driver()
    tweets_data = []
    
    # Track seen tweet URLs to avoid duplicates within this session
    seen_tweet_urls = set()
    
    # Initialize tracking variables
    start_time = time_module.time()
    max_time_seconds = max_time_minutes * 60
    last_tweets_count = 0
    consecutive_no_new_tweets = 0
    scroll_count = 0
    
    print(f"Opening Twitter to search for '{keyword}'...")
    search_url = f"https://x.com/search?q={keyword}&src=typed_query&f=live"
    driver.get(search_url)
    time.sleep(5)
    
    divxpath = '//div[@data-testid="cellInnerDiv"]'
    try:
        WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.XPATH, divxpath)))
    except Exception as e:
        print(f"Error waiting for tweets to load: {str(e)}")
        driver.quit()
        return tweets_data
    
    print(f"Starting infinite scroll to collect up to {max_tweets} new tweets (max time: {max_time_minutes} minutes)...")
    print(f"Skipping {len(existing_urls)} already scraped tweets")
    
    # Main scrolling loop
    while len(tweets_data) < max_tweets and (time_module.time() - start_time) < max_time_seconds:
        scroll_count += 1
        try:
            # Find all tweet elements currently on the page
            tweet_elements = driver.find_elements(By.XPATH, divxpath)
            
            new_tweets_found = False
            
            # Process visible tweets
            for tweet in tweet_elements:
                tweet_info = {}
                tweet_url = None
                
                try:
                    # Try to get tweet URL first to check if we've seen it
                    tweet_url = tweet.find_elements(By.XPATH, './/a[contains(@href,"status")]')[0].get_attribute('href')
                    
                    # Skip if we've already seen this tweet in this session or in previous runs
                    if tweet_url in seen_tweet_urls or tweet_url in existing_urls:
                        continue
                        
                    seen_tweet_urls.add(tweet_url)
                    tweet_info["DocURL"] = tweet_url
                    new_tweets_found = True
                    
                    # Now extract other data
                    profile = tweet.find_element(By.XPATH, './/div[@data-testid="User-Name"]')
                    tweet_info["Profile Link"] = profile.find_elements(By.XPATH, './/a')[1].get_attribute('href')
                    tweet_info["Profile Handle"] = profile.find_elements(By.XPATH, './/a')[1].text
                    
                    tweet_info["Post"] = tweet.find_element(By.XPATH, ".//div[@data-testid='tweetText']").text
                    
                    utc_datetime_str = tweet.find_element(By.XPATH, './/time').get_attribute('datetime')
                    tweet_info["Date"], tweet_info["Time"] = convert_to_ist(utc_datetime_str)
                    
                    tweets_data.append(tweet_info)
                    
                except Exception:
                    # If we couldn't get the URL or other required data, just skip this tweet
                    pass
            
            # Scroll down to load more tweets
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(2)  # Wait for new content to load
            
            # Check if we're getting new tweets
            if not new_tweets_found:
                consecutive_no_new_tweets += 1
                if consecutive_no_new_tweets >= 5:  # If no new tweets after 5 consecutive scrolls
                    print("No new tweets found after multiple scrolls. Probably reached the end or rate limited.")
                    break
            else:
                consecutive_no_new_tweets = 0
            
            # Print progress update every 20 tweets or 10 scrolls
            if len(tweets_data) % 20 == 0 or scroll_count % 10 == 0:
                elapsed_time = time_module.time() - start_time
                elapsed_formatted = str(timedelta(seconds=int(elapsed_time)))
                print(f"Scroll #{scroll_count}: Scraped {len(tweets_data)} new tweets so far. Elapsed time: {elapsed_formatted}")
                
                # Occasional longer pause to avoid rate limiting
                if scroll_count % 30 == 0:
                    print("Taking a short break to avoid rate limiting...")
                    time.sleep(5)
            
        except Exception as e:
            print(f"Error during scrolling: {str(e)}")
            # Try to recover from errors by scrolling a bit and continuing
            try:
                driver.execute_script("window.scrollBy(0, 500);")
                time.sleep(3)
            except:
                pass
    
    # Calculate and print final stats
    total_time = time_module.time() - start_time
    print(f"Scraping complete! Collected {len(tweets_data)} new tweets in {str(timedelta(seconds=int(total_time)))}")
    
    driver.quit()
    return tweets_data

def analyze_tweets(tweets_data):
    if not tweets_data:
        print("No tweets to analyze!")
        return pd.DataFrame()
        
    print(f"Analyzing {len(tweets_data)} tweets...")
    results = []
    tweets_text = [tweet["Post"] for tweet in tweets_data]
    similarity_scores = calculate_similarity_scores(tweets_text, intent_data)

    for i, tweet in enumerate(tweets_data):
        best_match_index = similarity_scores[i].argmax()
        best_match_score = similarity_scores[i, best_match_index].item()
        best_match_intent = intent_data[best_match_index]

        results.append({
            "Profile Handle": tweet.get("Profile Handle", ""),
            "Profile Link": tweet.get("Profile Link", ""),
            "DocURL": tweet.get("DocURL", ""),
            "Date": tweet.get("Date", ""),
            "Time": tweet.get("Time", ""),
            "Target Sentence": tweets_text[i],
            "Best Matched Intent": best_match_intent,
            "Similarity Score": round(best_match_score, 6)
        })

    df = pd.DataFrame(results)
    return df

def analyze_replies(replies_data):
    """Analyze replies using the same intent matching logic."""
    if not replies_data:
        return pd.DataFrame()
        
    results = []
    replies_text = [reply["Reply Text"] for reply in replies_data if "Reply Text" in reply]
    
    if not replies_text:
        return pd.DataFrame()
        
    similarity_scores = calculate_similarity_scores(replies_text, intent_data)

    for i, reply in enumerate(replies_data):
        if "Reply Text" not in reply:
            continue
            
        best_match_index = similarity_scores[i].argmax()
        best_match_score = similarity_scores[i, best_match_index].item()
        best_match_intent = intent_data[best_match_index]

        results.append({
            "Profile Handle": reply.get("Profile Handle", ""),
            "Profile Link": reply.get("Profile Link", ""),
            "ReplyURL": reply.get("ReplyURL", ""),
            "Original Tweet URL": reply.get("Original Tweet URL", ""),
            "Date": reply.get("Date", ""),
            "Time": reply.get("Time", ""),
            "Reply Text": reply.get("Reply Text", ""),
            "Best Matched Intent": best_match_intent,
            "Similarity Score": round(best_match_score, 6)
        })

    df = pd.DataFrame(results)
    return df

if __name__ == "__main__":
    # Configuration
    keyword = "Artificial Intelligence"
    max_tweets = 100  # Reduced to focus on replies
    max_runtime_minutes = 30  # Maximum runtime in minutes
    max_replies_per_tweet = 50  # Increased max replies to collect per tweet
    spreadsheet_name = "Twitter_AI_Analysis"
    
    # Define filenames for local storage
    tweets_csv_filename = f"Twitter_{keyword.replace(' ', '_')}_tweets.csv"
    replies_csv_filename = f"Twitter_{keyword.replace(' ', '_')}_replies.csv"
    
    start_time = time_module.time()
    
    # Get existing URLs to avoid re-scraping
    existing_tweet_urls, existing_reply_urls = get_existing_urls(tweets_csv_filename, replies_csv_filename)
    
    # Option to skip tweet scraping and use existing tweets for reply scraping
    skip_tweet_scraping = False  # Set to True if you want to skip tweet scraping
    
    if not skip_tweet_scraping:
        # Scrape new tweets with infinite scrolling
        print(f"Starting Twitter scraper for keyword '{keyword}'")
        new_tweets_data = scrape_tweets_with_metadata(keyword, existing_urls=existing_tweet_urls, 
                                                   max_tweets=max_tweets, max_time_minutes=max_runtime_minutes)
        
        if new_tweets_data:
            # Analyze new tweets
            new_tweets_df = analyze_tweets(new_tweets_data)
            
            # Append to local CSV
            updated_tweets_df = append_to_csv(new_tweets_df, tweets_csv_filename)
            
            # Append to Google Sheets
            tweets_sheet_url, _ = append_to_sheets(new_tweets_df, spreadsheet_name, "Tweets")
            if tweets_sheet_url:
                print(f"Tweets data appended to Google Sheets: {tweets_sheet_url}")
        else:
            print("No new tweets were collected.")
            # If no new tweets, use existing ones for reply scraping
            try:
                updated_tweets_df = pd.read_csv(tweets_csv_filename)
            except:
                print("No existing tweets found either. Exiting.")
                exit()
    else:
        # Use existing tweets for reply scraping
        try:
            updated_tweets_df = pd.read_csv(tweets_csv_filename)
            print(f"Using {len(updated_tweets_df)} existing tweets for reply scraping.")
        except:
            print("No existing tweets found. Please run without skip_tweet_scraping=True first.")
            exit()
    
    # Get tweet URLs to scrape replies from
    tweet_urls_for_replies = []
    
    if not skip_tweet_scraping and new_tweets_data:
        # First priority: use newly scraped tweets
        for tweet in new_tweets_data[:20]:  # Limit to first 20 new tweets
            if "DocURL" in tweet:
                tweet_urls_for_replies.append(tweet["DocURL"])
    
    # If we don't have enough from new tweets, supplement with existing tweets
    if len(tweet_urls_for_replies) < 20:
        if 'DocURL' in updated_tweets_df.columns:
            # Sort by recency (if Date and Time columns exist)
            if 'Date' in updated_tweets_df.columns and 'Time' in updated_tweets_df.columns:
                updated_tweets_df['DateTime'] = pd.to_datetime(updated_tweets_df['Date'] + ' ' + updated_tweets_df['Time'], 
                                                            errors='coerce')
                updated_tweets_df = updated_tweets_df.sort_values('DateTime', ascending=False)
            
            # Get more URLs from existing tweets
            more_urls = updated_tweets_df['DocURL'].tolist()
            for url in more_urls:
                if url not in tweet_urls_for_replies:
                    tweet_urls_for_replies.append(url)
                    if len(tweet_urls_for_replies) >= 20:
                        break
    
    # Now scrape replies for our collected tweet URLs
    print(f"Starting to scrape replies for {len(tweet_urls_for_replies)} tweets...")
    
    all_new_replies = []
    for i, tweet_url in enumerate(tweet_urls_for_replies):
        print(f"\nScraping replies for tweet {i+1}/{len(tweet_urls_for_replies)}")
        print(f"Tweet URL: {tweet_url}")
        
        tweet_replies = scrape_tweet_replies(tweet_url, 
                                          existing_reply_urls=existing_reply_urls,
                                          max_replies=max_replies_per_tweet)
        
        if tweet_replies:
            print(f"Successfully scraped {len(tweet_replies)} replies for this tweet")
            all_new_replies.extend(tweet_replies)
        else:
            print("No replies found or scraped for this tweet")
        
        # Add a delay between tweets to avoid rate limiting
        time.sleep(5)
    
    if all_new_replies:
        # Analyze new replies
        new_replies_df = analyze_replies(all_new_replies)
        
        # Append to local CSV
        updated_replies_df = append_to_csv(new_replies_df, replies_csv_filename)
        
        # Append to Google Sheets
        replies_sheet_url, _ = append_to_sheets(new_replies_df, spreadsheet_name, "Replies")
        if replies_sheet_url:
            print(f"Replies data appended to Google Sheets: {replies_sheet_url}")
            
        print(f"Total new replies scraped: {len(all_new_replies)}")
    else:
        print("No new replies were found or scraped.")
    
    # Print final execution time
    total_time = time_module.time() - start_time
    print(f"Total execution time: {str(timedelta(seconds=int(total_time)))}")
    
    # Print summary
    try:
        total_tweets = pd.read_csv(tweets_csv_filename).shape[0]
        print(f"Total tweets in database: {total_tweets}")
        
        if os.path.exists(replies_csv_filename):
            total_replies = pd.read_csv(replies_csv_filename).shape[0]
            print(f"Total replies in database: {total_replies}")
    except Exception as e:
        print(f"Error getting final stats: {str(e)}")
    # Configuration
    keyword = "Artificial Intelligence"
    max_tweets = 500  # Set your desired maximum number of new tweets
    max_runtime_minutes = 45  # Maximum runtime in minutes
    max_replies_per_tweet = 30  # Maximum replies to collect per tweet
    spreadsheet_name = "Twitter_AI_Analysis"
    
    # Define filenames for local storage
    tweets_csv_filename = f"Twitter_{keyword.replace(' ', '_')}_tweets.csv"
    replies_csv_filename = f"Twitter_{keyword.replace(' ', '_')}_replies.csv"
    
    start_time = time_module.time()
    
    # Get existing URLs to avoid re-scraping
    existing_tweet_urls, existing_reply_urls = get_existing_urls(tweets_csv_filename, replies_csv_filename)
    
    # Scrape new tweets with infinite scrolling
    print(f"Starting Twitter scraper for keyword '{keyword}'")
    new_tweets_data = scrape_tweets_with_metadata(keyword, existing_urls=existing_tweet_urls, 
                                               max_tweets=max_tweets, max_time_minutes=max_runtime_minutes)
    
    if new_tweets_data:
        # Analyze new tweets
        new_tweets_df = analyze_tweets(new_tweets_data)
        
        # Append to local CSV
        updated_tweets_df = append_to_csv(new_tweets_df, tweets_csv_filename)
        
        # Append to Google Sheets
        tweets_sheet_url, _ = append_to_sheets(new_tweets_df, spreadsheet_name, "Tweets")
        if tweets_sheet_url:
            print(f"Tweets data appended to Google Sheets: {tweets_sheet_url}")
    else:
        print("No new tweets were collected.")
        
    # Optional: Scrape replies for new tweets
    scrape_replies = True  # Set to False if you don't want to scrape replies
    
    if scrape_replies and new_tweets_data:
        print(f"Starting to scrape replies for {len(new_tweets_data)} new tweets...")
        # Limit to first 10 tweets to avoid excessive runtime
        tweets_for_replies = new_tweets_data[:10]
        
        all_new_replies = []
        for i, tweet in enumerate(tweets_for_replies):
            if "DocURL" in tweet:
                print(f"Scraping replies for tweet {i+1}/{len(tweets_for_replies)}")
                tweet_replies = scrape_tweet_replies(tweet["DocURL"], 
                                                    existing_reply_urls=existing_reply_urls,
                                                    max_replies=max_replies_per_tweet)
                all_new_replies.extend(tweet_replies)
                # Add a small delay to avoid rate limiting
                time.sleep(3)
        
        if all_new_replies:
            # Analyze new replies
            new_replies_df = analyze_replies(all_new_replies)
            
            # Append to local CSV
            updated_replies_df = append_to_csv(new_replies_df, replies_csv_filename)
            
            # Append to Google Sheets
            replies_sheet_url, _ = append_to_sheets(new_replies_df, spreadsheet_name, "Replies")
            if replies_sheet_url:
                print(f"Replies data appended to Google Sheets: {replies_sheet_url}")
                
            print(f"Total new replies scraped: {len(all_new_replies)}")
        else:
            print("No new replies were found or scraped.")
    
    # Print final execution time
    total_time = time_module.time() - start_time
    print(f"Total execution time: {str(timedelta(seconds=int(total_time)))}")
    
    # Print summary
    try:
        total_tweets = pd.read_csv(tweets_csv_filename).shape[0]
        print(f"Total tweets in database: {total_tweets}")
        
        if os.path.exists(replies_csv_filename):
            total_replies = pd.read_csv(replies_csv_filename).shape[0]
            print(f"Total replies in database: {total_replies}")
    except Exception as e:
        print(f"Error getting final stats: {str(e)}")