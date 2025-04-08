import pandas as pd
import time
import pytz
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
from gspread_dataframe import set_with_dataframe

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
    creds = ServiceAccountCredentials.from_json_keyfile_name('credentials.json', scope)
    client = gspread.authorize(creds)
    
    return client

def upload_to_sheets(df, sheet_name, tab_name):
    """Upload dataframe to Google Sheet."""
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
        except gspread.exceptions.WorksheetNotFound:
            worksheet = sheet.add_worksheet(title=tab_name, rows="1000", cols="20")
        
        # Clear existing data
        worksheet.clear()
        
        # Upload dataframe
        set_with_dataframe(worksheet, df)
        
        sheet_url = f"https://docs.google.com/spreadsheets/d/{sheet.id}"
        print(f"Data uploaded successfully to Google Sheets: {sheet_url}")
        
        return sheet_url
    
    except Exception as e:
        print(f"Error uploading to Google Sheets: {str(e)}")
        return None

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

def scrape_tweet_replies(tweet_url):
    """Scrape replies for a specific tweet."""
    driver = setup_driver()
    print(f"Opening tweet URL: {tweet_url}")
    driver.get(tweet_url)
    time.sleep(5)
    
    replies_data = []
    
    try:
        # Wait for replies to load
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, '//div[@data-testid="cellInnerDiv"]'))
        )
        
        # Skip the first element as it's the original tweet
        reply_elements = driver.find_elements(By.XPATH, '//div[@data-testid="cellInnerDiv"]')[1:]
        
        for reply in reply_elements:
            reply_info = {}
            
            try:
                profile = reply.find_element(By.XPATH, './/div[@data-testid="User-Name"]')
                reply_info["Profile Link"] = profile.find_elements(By.XPATH, './/a')[1].get_attribute('href')
                reply_info["Profile Handle"] = profile.find_elements(By.XPATH, './/a')[1].text
            except Exception:
                pass
                
            try:
                reply_info["Reply Text"] = reply.find_element(By.XPATH, ".//div[@data-testid='tweetText']").text
            except Exception:
                pass
                
            try:
                utc_datetime_str = reply.find_element(By.XPATH, './/time').get_attribute('datetime')
                reply_info["Date"], reply_info["Time"] = convert_to_ist(utc_datetime_str)
            except Exception:
                pass
                
            try:
                reply_info["ReplyURL"] = reply.find_elements(By.XPATH, './/a[contains(@href,"status")]')[0].get_attribute('href')
            except Exception:
                pass
                
            if "Reply Text" in reply_info:
                reply_info["Original Tweet URL"] = tweet_url
                replies_data.append(reply_info)
            
            # Scroll down to load more replies
            if len(replies_data) % 5 == 0:
                driver.execute_script("window.scrollBy(0, 500)")
                time.sleep(2)
                
            # Limit the number of replies per tweet
            if len(replies_data) >= 10:
                break
                
    except Exception as e:
        print(f"Error scraping replies: {str(e)}")
    
    driver.quit()
    return replies_data

def scrape_tweets_with_metadata(keyword, num_tweets=100):
    driver = setup_driver()

    print("Opening Twitter...")
    driver.get("https://x.com")
    time.sleep(5)

    search_url = f"https://x.com/search?q={keyword}&src=typed_query&f=live"
    driver.get(search_url)
    time.sleep(5)

    divxpath = '//div[@data-testid="cellInnerDiv"]'
    WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.XPATH, divxpath)))

    row = driver.find_element(By.XPATH, divxpath)
    action = ActionChains(driver)

    tweets_data = []

    while len(tweets_data) < num_tweets:
        try:
            nexts = driver.execute_script("return arguments[0].nextSibling;", row)
            try:
                action.move_to_element(nexts).perform()
            except Exception:
                pass

            tweet_info = {}

            try:
                profile = row.find_element(By.XPATH, './/div[@data-testid="User-Name"]')
                tweet_info["Profile Link"] = profile.find_elements(By.XPATH, './/a')[1].get_attribute('href')
                tweet_info["Profile Handle"] = profile.find_elements(By.XPATH, './/a')[1].text
            except Exception:
                pass

            try:
                tweet_info["Post"] = row.find_element(By.XPATH, ".//div[@data-testid='tweetText']").text
            except Exception:
                pass

            try:
                tweet_info["DocURL"] = row.find_elements(By.XPATH, './/a[contains(@href,"status")]')[0].get_attribute('href')
            except Exception:
                pass

            try:
                utc_datetime_str = row.find_element(By.XPATH, './/time').get_attribute('datetime')
                tweet_info["Date"], tweet_info["Time"] = convert_to_ist(utc_datetime_str)
            except Exception:
                pass

            if "Post" in tweet_info and "DocURL" in tweet_info:
                tweets_data.append(tweet_info)

            row = nexts

            if len(tweets_data) % 10 == 0:
                driver.find_element(By.TAG_NAME, "body").send_keys(Keys.PAGE_DOWN)
                time.sleep(3)

        except Exception:
            try:
                driver.find_element(By.TAG_NAME, "body").send_keys(Keys.PAGE_DOWN)
                time.sleep(2)
                row = driver.find_element(By.XPATH, divxpath)
                nexts = driver.execute_script("return arguments[0].nextSibling;", row)
            except Exception:
                pass

    driver.quit()
    print(f"Scraped {len(tweets_data)} tweets.")
    return tweets_data

def analyze_tweets(tweets_data):
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
    keyword = "Artificial Intelligence"
    spreadsheet_name = "Twitter_AI_Analysis"
    
    # Scrape and analyze tweets
    tweets_data = scrape_tweets_with_metadata(keyword, num_tweets=5)
    tweets_df = analyze_tweets(tweets_data)
    
    # Save tweets data locally
    tweets_csv_filename = "Test_result1.csv"
    tweets_df.to_csv(tweets_csv_filename, index=False)
    print(f"Tweet analysis complete! Saved locally to {tweets_csv_filename}")
    
    # Upload tweets data to Google Sheets
    tweets_sheet_url = upload_to_sheets(tweets_df, spreadsheet_name, "Tweets")
    if tweets_sheet_url:
        print(f"Tweets data uploaded to Google Sheets: {tweets_sheet_url}")
    
    # Scrape and analyze replies
    all_replies = []
    for i, tweet in enumerate(tweets_data):
        if "DocURL" in tweet:
            print(f"Scraping replies for tweet {i+1}/{len(tweets_data)}")
            tweet_replies = scrape_tweet_replies(tweet["DocURL"])
            all_replies.extend(tweet_replies)
            # Add a small delay to avoid rate limiting
            time.sleep(2)
    
    if all_replies:
        replies_df = analyze_replies(all_replies)
        
        # Save replies data locally
        replies_csv_filename = "Tweet_replies.csv"
        replies_df.to_csv(replies_csv_filename, index=False)
        print(f"Replies analysis complete! Saved locally to {replies_csv_filename}")
        
        # Upload replies data to Google Sheets
        replies_sheet_url = upload_to_sheets(replies_df, spreadsheet_name, "Replies")
        if replies_sheet_url:
            print(f"Replies data uploaded to Google Sheets: {replies_sheet_url}")
            
        print(f"Total replies scraped: {len(all_replies)}")
    else:
        print("No replies were found or scraped.")