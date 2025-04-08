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
import random
import re
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import os

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
    """
    Set up Google Sheets API connection
    """
    print("Setting up Google Sheets connection...")
    try:
        # Define the scope
        scope = ['https://spreadsheets.google.com/feeds',
                'https://www.googleapis.com/auth/drive']

        # Add credentials to the account
        credentials = ServiceAccountCredentials.from_json_keyfile_name(
            'google-sheets-bot@affable-cacao-454507-a2.iam.gserviceaccount.com', scope)  # Make sure this file exists in your directory

        # Authorize the clientsheet
        client = gspread.authorize(credentials)
        
        return client
    except Exception as e:
        print(f"Error setting up Google Sheets: {str(e)}")
        return None

def upload_to_sheets(client, data_df, sheet_name, spreadsheet_key=None):
    """
    Upload DataFrame to Google Sheets
    """
    try:
        if not isinstance(data_df, pd.DataFrame) or data_df.empty:
            print(f"No data to upload to {sheet_name}")
            return
            
        print(f"Uploading data to Google Sheets: {sheet_name}")
        
        # If spreadsheet_key is provided, open existing spreadsheet
        # Otherwise create a new one
        if spreadsheet_key:
            spreadsheet = client.open_by_key(spreadsheet_key)
        else:
            spreadsheet = client.create(f"LinkedIn Data - {datetime.now().strftime('%Y-%m-%d')}")
            print(f"Created new spreadsheet with ID: {spreadsheet.id}")
            # Make it accessible to anyone with the link
            spreadsheet.share(None, perm_type='anyone', role='reader')
        
        # Check if worksheet exists, if not create it
        try:
            worksheet = spreadsheet.worksheet(sheet_name)
            # Clear existing content
            worksheet.clear()
        except gspread.exceptions.WorksheetNotFound:
            worksheet = spreadsheet.add_worksheet(title=sheet_name, rows=data_df.shape[0] + 10, cols=data_df.shape[1] + 5)
        
        # Convert DataFrame to list of lists
        data_to_upload = [data_df.columns.tolist()] + data_df.values.tolist()
        
        # Update the sheet
        worksheet.update(data_to_upload)
        
        print(f"Successfully uploaded {len(data_df)} rows to {sheet_name}")
        return spreadsheet.id
        
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
    
    # Add random user agent to avoid detection
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36"
    ]
    chrome_options.add_argument(f"user-agent={random.choice(user_agents)}")

    driver = uc.Chrome(options=chrome_options)
    return driver

def clean_timestamp(timestamp_text):
    """
    Clean the timestamp text and add 'ago' if not present
    """
    if not timestamp_text:
        return ""
    
    # Clean special characters
    clean_text = re.sub(r'[^\w\s•\-:]', '', timestamp_text)
    
    # Replace bullet character with simple dot
    clean_text = clean_text.replace('•', '·')
    
    # Extract just the time part if it follows a common pattern
    time_match = re.search(r'(\d+[hmdw])', clean_text)
    if time_match:
        time_part = time_match.group(1)
        # Add "ago" if it's not already there
        if not "ago" in clean_text.lower():
            return f"{time_part} ago"
        return clean_text.strip()
    
    # If no specific time pattern found but has content, add "ago" if not present
    if clean_text.strip() and not "ago" in clean_text.lower():
        return f"{clean_text.strip()} ago"
        
    return clean_text.strip()

def safe_find_element(driver, by, value, wait_time=5):
    """Safely find an element with better error handling."""
    try:
        element = WebDriverWait(driver, wait_time).until(
            EC.presence_of_element_located((by, value))
        )
        return element
    except Exception:
        return None

def safe_find_elements(driver, by, value, wait_time=5):
    """Safely find elements with better error handling."""
    try:
        WebDriverWait(driver, wait_time).until(
            EC.presence_of_element_located((by, value))
        )
        elements = driver.find_elements(by, value)
        return elements
    except Exception:
        return []

def safe_click(driver, element):
    """Safely click an element using multiple approaches."""
    try:
        element.click()
        return True
    except Exception:
        try:
            driver.execute_script("arguments[0].click();", element)
            return True
        except Exception:
            try:
                ActionChains(driver).move_to_element(element).click().perform()
                return True
            except Exception:
                return False

def scrape_linkedin_post_comments(post_url):
    """Scrape comments for a specific LinkedIn post."""
    driver = setup_driver()
    print(f"Opening LinkedIn post URL: {post_url}")
    
    try:
        driver.get(post_url)
        # Random sleep between 4-7 seconds to mimic human behavior
        time.sleep(random.uniform(4, 7))
        
        comments_data = []
        
        # Check if there are comments and click to expand if needed
        see_more_buttons = safe_find_elements(driver, By.XPATH, 
            '//button[contains(@class, "comments-comments-list__show-previous") or contains(text(), "Load more comments")]')
        
        for button in see_more_buttons[:3]:  # Limit to 3 "load more" clicks
            if safe_click(driver, button):
                time.sleep(random.uniform(2, 4))
        
        # Multiple XPath options for comments
        comment_xpath_options = [
            '//article[contains(@class, "comments-comment-item")]',
            '//div[contains(@class, "comments-comment-item")]',
            '//div[contains(@class, "scaffold-finite-scroll__content")]//div[contains(@class, "comments-comment-item")]',
            '//div[contains(@data-test-id, "comments-container")]//article'
        ]
        
        comment_elements = []
        for xpath in comment_xpath_options:
            comment_elements = safe_find_elements(driver, By.XPATH, xpath)
            if comment_elements:
                break
        
        for comment in comment_elements:
            comment_info = {}
            
            # Multiple XPath options for profile info
            profile_xpath_options = [
                './/a[contains(@class, "comments-post-meta__actor-link")]',
                './/a[contains(@class, "tap-target")]',
                './/a[contains(@class, "comment-actor")]',
                './/a[contains(@href, "/in/")]'
            ]
            
            profile_element = None
            for xpath in profile_xpath_options:
                profile_elements = comment.find_elements(By.XPATH, xpath)
                if profile_elements:
                    profile_element = profile_elements[0]
                    break
            
            if profile_element:
                comment_info["Profile Link"] = profile_element.get_attribute('href')
                
                # Get the author name more reliably
                name_xpath_options = [
                    './/span[contains(@class, "comments-post-meta__name")]',
                    './/span[contains(@class, "feed-shared-actor__name")]',
                    './/span[contains(@class, "hoverable-link-text")]',
                    './/span[contains(@aria-hidden, "true")]'
                ]
                
                author_name = ""
                for name_xpath in name_xpath_options:
                    name_elements = comment.find_elements(By.XPATH, name_xpath)
                    if name_elements:
                        author_name = name_elements[0].text.strip()
                        break
                
                # If we got a name, use it, otherwise use the profile element text
                if author_name:
                    comment_info["Profile Handle"] = author_name
                else:
                    comment_info["Profile Handle"] = profile_element.text.strip()
            
            # Multiple XPath options for comment text
            text_xpath_options = [
                './/div[contains(@class, "comments-comment-item__main-content")]',
                './/div[contains(@class, "feed-shared-text")]',
                './/span[contains(@class, "comments-comment-item__main-content")]',
                './/div[contains(@class, "comments-comment-text-container")]',
                './/p'
            ]
            
            for xpath in text_xpath_options:
                text_elements = comment.find_elements(By.XPATH, xpath)
                if text_elements:
                    comment_info["Comment Text"] = text_elements[0].text.strip()
                    break
            
            # Multiple XPath options for timestamp
            time_xpath_options = [
                './/span[contains(@class, "comments-comment-item__timestamp")]',
                './/time',
                './/span[contains(@class, "feed-shared-actor__sub-description")]',
                './/span[contains(@class, "artdeco-text-duration")]'
            ]
            
            for xpath in time_xpath_options:
                time_elements = comment.find_elements(By.XPATH, xpath)
                if time_elements:
                    raw_timestamp = time_elements[0].text
                    comment_info["Timestamp"] = clean_timestamp(raw_timestamp)
                    break
            
            if "Comment Text" in comment_info:
                comment_info["Original Post URL"] = post_url
                comments_data.append(comment_info)
            
            # Limit the number of comments per post
            if len(comments_data) >= 10:
                break
                
    except Exception as e:
        print(f"Error scraping comments: {str(e)}")
    
    driver.quit()
    return comments_data

def extract_profile_handle(driver, post):
    """Extract profile handle with multiple approaches and better error handling"""
    try:
        # Try multiple XPath patterns for profile name
        profile_name_xpath_options = [
            './/span[contains(@class, "update-components-actor__name")]',
            './/span[contains(@class, "feed-shared-actor__name")]',
            './/span[contains(@class, "update-components-actor__title")]',
            './/a[contains(@class, "app-aware-link") and contains(@href, "/in/")]//span',
            './/a[contains(@href, "/in/")]//span[1]'
        ]
        
        for xpath in profile_name_xpath_options:
            name_elements = post.find_elements(By.XPATH, xpath)
            if name_elements:
                name = name_elements[0].text.strip()
                if name:
                    return name
        
        # If all XPath attempts fail, try JavaScript to get text content
        try:
            # Find any element that's likely to contain the name
            possible_name_elements = post.find_elements(By.XPATH, 
                './/a[contains(@href, "/in/")]')
            
            if possible_name_elements:
                # Try to get the text content using JavaScript
                name = driver.execute_script("return arguments[0].textContent", possible_name_elements[0])
                if name:
                    return name.strip()
        except:
            pass
            
        return "Unknown Profile"
    except Exception as e:
        print(f"Error extracting profile handle: {str(e)}")
        return "Unknown Profile"

def extract_profile_link(post):
    """Extract profile link with multiple approaches and better error handling"""
    try:
        # Try multiple XPath patterns for profile link
        profile_link_xpath_options = [
            './/div[contains(@class, "update-components-actor")]//a[contains(@class, "update-components-actor__container-link")]',
            './/a[contains(@class, "app-aware-link") and contains(@href, "/in/")]',
            './/a[contains(@class, "feed-shared-actor__container-link")]',
            './/span[contains(@class, "update-components-actor__name")]//ancestor::a[contains(@href, "/in/")]',
            './/a[contains(@href, "/in/")]'
        ]
        
        for xpath in profile_link_xpath_options:
            link_elements = post.find_elements(By.XPATH, xpath)
            if link_elements:
                href = link_elements[0].get_attribute('href')
                if href and "/in/" in href:
                    return href
        
        return ""
    except Exception as e:
        print(f"Error extracting profile link: {str(e)}")
        return ""

def scrape_linkedin_posts(keyword, num_posts=100):
    driver = setup_driver()

    print("Opening LinkedIn...")
    
    try:
        # First go to LinkedIn homepage to ensure we're logged in
        driver.get("https://www.linkedin.com")
        time.sleep(random.uniform(4, 7))
        
        # Try different search approaches
        search_urls = [
            f"https://www.linkedin.com/search/results/content/?keywords={keyword}&origin=GLOBAL_SEARCH_HEADER",
            f"https://www.linkedin.com/feed/",  # Go to feed and then search
            f"https://www.linkedin.com/feed/hashtag/{keyword}/"  # Try hashtag search
        ]
        
        for url in search_urls:
            try:
                driver.get(url)
                time.sleep(random.uniform(5, 8))
                
                if "feed/hashtag" not in url and "search/results" in url:
                    # Only try to sort if we're on search results page
                    try:
                        # Try different approaches to sort by recent
                        sort_buttons = safe_find_elements(driver, By.XPATH, 
                            '//button[contains(@class, "search-reusables__filter-trigger") or contains(@aria-label, "Sort by")]')
                        
                        if sort_buttons:
                            safe_click(driver, sort_buttons[0])
                            time.sleep(random.uniform(1, 2))
                            
                            recent_options = safe_find_elements(driver, By.XPATH, 
                                '//span[text()="Recent" or contains(text(), "Most recent")]')
                            
                            if recent_options:
                                safe_click(driver, recent_options[0])
                                time.sleep(random.uniform(3, 5))
                                print("Successfully sorted by recent.")
                    except Exception as e:
                        print(f"Could not switch to recent posts, continuing with default sort: {str(e)}")
                
                # Test if we can find any posts
                post_elements = safe_find_elements(driver, By.XPATH, '//div[contains(@class, "feed-shared-update-v2")]')
                if post_elements:
                    print(f"Found {len(post_elements)} initial posts on {url}")
                    break  # Found posts, continue with this URL
                else:
                    post_elements = safe_find_elements(driver, By.XPATH, '//div[contains(@class, "scaffold-finite-scroll__content")]//div[contains(@class, "feed-shared")]')
                    if post_elements:
                        print(f"Found {len(post_elements)} initial posts on {url} (alternative selector)")
                        break
            
            except Exception as e:
                print(f"Error with URL {url}: {str(e)}")
        
        posts_data = []
        last_height = driver.execute_script("return document.body.scrollHeight")
        no_new_posts_count = 0
        scroll_attempts = 0
        max_scroll_attempts = 30
        
        # Multiple XPath options for finding posts
        post_xpath_options = [
            '//div[contains(@class, "feed-shared-update-v2")]',
            '//div[contains(@class, "update-components-actor")]//ancestor::div[contains(@class, "feed-shared")]',
            '//div[contains(@class, "scaffold-finite-scroll__content")]//div[contains(@data-urn, "urn:li:activity")]',
            '//div[contains(@class, "search-results__cluster-content")]//div[contains(@class, "feed-shared")]'
        ]

        while len(posts_data) < num_posts and scroll_attempts < max_scroll_attempts and no_new_posts_count < 5:
            scroll_attempts += 1
            
            # Try different XPath selectors for posts
            post_elements = []
            for xpath in post_xpath_options:
                post_elements = safe_find_elements(driver, By.XPATH, xpath)
                if post_elements:
                    break
            
            initial_count = len(posts_data)
            
            for post in post_elements:
                if len(posts_data) >= num_posts:
                    break
                    
                post_info = {}
                
                # Get profile handle more reliably
                post_info["Profile Handle"] = extract_profile_handle(driver, post)
                
                # Get profile link more reliably - use the dedicated function
                post_info["Profile Link"] = extract_profile_link(post)
                
                # Try to get post content with multiple selectors
                content_xpath_options = [
                    './/div[contains(@class, "update-components-text")]',
                    './/div[contains(@class, "feed-shared-update-v2__description")]',
                    './/div[contains(@class, "feed-shared-text")]',
                    './/span[contains(@class, "break-words")]'
                ]
                
                for xpath in content_xpath_options:
                    content_elements = post.find_elements(By.XPATH, xpath)
                    if content_elements:
                        post_info["Post"] = content_elements[0].text.strip()
                        break
                
                # Try to get post URL with multiple selectors
                url_xpath_options = [
                    './/a[contains(@class, "app-aware-link") and contains(@href, "/feed/update/")]',
                    './/a[contains(@href, "activities/shares")]',
                    './/div[contains(@class, "feed-shared-control-menu")]//ancestor::div[contains(@data-urn, "urn:li:activity")]'
                ]
                
                for xpath in url_xpath_options:
                    url_elements = post.find_elements(By.XPATH, xpath)
                    if url_elements:
                        if xpath.endswith('data-urn, "urn:li:activity")]'):
                            # Extract post ID from data-urn attribute
                            try:
                                urn = post.get_attribute('data-urn')
                                if urn and ":" in urn:
                                    activity_id = urn.split(":")[-1]
                                    post_info["DocURL"] = f"https://www.linkedin.com/feed/update/urn:li:activity:{activity_id}"
                            except:
                                pass
                        else:
                            post_info["DocURL"] = url_elements[0].get_attribute('href')
                        break
                
                # Try to get timestamp with multiple selectors
                time_xpath_options = [
                    './/span[contains(@class, "update-components-actor__sub-description")]',
                    './/span[contains(@class, "feed-shared-actor__sub-description")]',
                    './/span[contains(@class, "visually-hidden") and contains(text(), "ago")]',
                    './/time'
                ]
                
                for xpath in time_xpath_options:
                    time_elements = post.find_elements(By.XPATH, xpath)
                    if time_elements:
                        raw_timestamp = time_elements[0].text
                        post_info["Timestamp"] = clean_timestamp(raw_timestamp)
                        break
                
                # Check if we have enough data and this post is unique
                if "Post" in post_info and "DocURL" in post_info:
                    # Check if this post is already in our data to avoid duplicates
                    is_duplicate = False
                    for existing_post in posts_data:
                        if existing_post.get("DocURL") == post_info.get("DocURL"):
                            is_duplicate = True
                            break
                    
                    if not is_duplicate:
                        posts_data.append(post_info)
                        print(f"Found post {len(posts_data)}/{num_posts}")
            
            # Scroll down to load more posts with randomized scroll amount
            scroll_amount = random.randint(300, 1000)
            driver.execute_script(f"window.scrollBy(0, {scroll_amount});")
            time.sleep(random.uniform(2, 4))
            
            # Check if new posts were found in this iteration
            if len(posts_data) == initial_count:
                no_new_posts_count += 1
            else:
                no_new_posts_count = 0
            
            new_height = driver.execute_script("return document.body.scrollHeight")
            if new_height == last_height:
                # If no new content loaded after scrolling, try to click "Show more" button if exists
                try:
                    show_more_buttons = safe_find_elements(driver, By.XPATH, 
                        '//button[contains(@class, "scaffold-finite-scroll__load-button") or contains(text(), "Show more results")]')
                    
                    if show_more_buttons:
                        safe_click(driver, show_more_buttons[0])
                        time.sleep(random.uniform(3, 5))
                    else:
                        no_new_posts_count += 1
                except Exception:
                    no_new_posts_count += 1
            
            last_height = new_height
        
    except Exception as e:
        print(f"Error in scrape_linkedin_posts: {str(e)}")
    finally:
        driver.quit()
    
    print(f"Scraped {len(posts_data)} LinkedIn posts.")
    return posts_data

def analyze_posts(posts_data):
    results = []
    posts_text = [post["Post"] for post in posts_data if "Post" in post]
    
    if not posts_text:
        return pd.DataFrame()
        
    similarity_scores = calculate_similarity_scores(posts_text, intent_data)

    for i, post in enumerate(posts_data):
        if "Post" not in post:
            continue
            
        best_match_index = similarity_scores[i].argmax()
        best_match_score = similarity_scores[i, best_match_index].item()
        best_match_intent = intent_data[best_match_index]

        results.append({
            "Profile Handle": post.get("Profile Handle", "Unknown"),
            "Profile Link": post.get("Profile Link", ""),  # Ensure profile link is stored
            "DocURL": post.get("DocURL", ""),
            "Timestamp": post.get("Timestamp", ""),
            "Target Sentence": post.get("Post", ""),
            "Best Matched Intent": best_match_intent,
            "Similarity Score": round(best_match_score, 6)
        })

    df = pd.DataFrame(results)
    return df

def analyze_comments(comments_data):
    """Analyze comments using the same intent matching logic."""
    if not comments_data:
        return pd.DataFrame()
        
    results = []
    comments_text = [comment["Comment Text"] for comment in comments_data if "Comment Text" in comment]
    
    if not comments_text:
        return pd.DataFrame()
        
    similarity_scores = calculate_similarity_scores(comments_text, intent_data)

    for i, comment in enumerate(comments_data):
        if "Comment Text" not in comment:
            continue
            
        best_match_index = similarity_scores[i].argmax()
        best_match_score = similarity_scores[i, best_match_index].item()
        best_match_intent = intent_data[best_match_index]

        results.append({
            "Profile Handle": comment.get("Profile Handle", "Unknown"),
            "Profile Link": comment.get("Profile Link", ""),
            "Original Post URL": comment.get("Original Post URL", ""),
            "Comment Text": comment.get("Comment Text", ""),
            "Best Matched Intent": best_match_intent,
            "Similarity Score": round(best_match_score, 6)
        })

    df = pd.DataFrame(results)
    return df

def main():
    try:
        print("Starting LinkedIn scraper...")
        
        # Initialize Google Sheets client
        sheets_client = setup_google_sheets()
        if not sheets_client:
            print("Failed to set up Google Sheets connection. Will still save data locally.")
        
        # Get spreadsheet key from environment variable or use default
        spreadsheet_key = os.environ.get('GOOGLE_SPREADSHEET_KEY', None)
        
        keyword = "Artificial Intelligence"
        num_posts = 5  # Start with a small number to test
        
        posts_data = scrape_linkedin_posts(keyword, num_posts=num_posts)
        
        if not posts_data:
            print("No posts were found. Trying alternative approach...")
            # Try a different keyword or approach
            keyword = "AI"
            posts_data = scrape_linkedin_posts(keyword, num_posts=num_posts)
            
        # Save posts data
        if posts_data:
            posts_df = analyze_posts(posts_data)
            
            # Save to local CSV
            posts_csv_filename = "LinkedIn_posts_result.csv"
            posts_df.to_csv(posts_csv_filename, index=False)
            print(f"LinkedIn posts analysis complete! Saved to {posts_csv_filename}")
            
            # Upload to Google Sheets
            if sheets_client:
                spreadsheet_key = upload_to_sheets(sheets_client, posts_df, "LinkedIn Posts", spreadsheet_key)
                print(f"Posts data uploaded to Google Sheets with ID: {spreadsheet_key}")
            
            # Scrape and save comments data
            all_comments = []
            for i, post in enumerate(posts_data):
                if "DocURL" in post:
                    print(f"Scraping comments for post {i+1}/{len(posts_data)}")
                    post_comments = scrape_linkedin_post_comments(post["DocURL"])
                    all_comments.extend(post_comments)
                    # Add a small delay to avoid rate limiting
                    time.sleep(random.uniform(2, 4))
            
            if all_comments:
                comments_df = analyze_comments(all_comments)
                
                # Save to local CSV
                comments_csv_filename = "LinkedIn_comments.csv"
                comments_df.to_csv(comments_csv_filename, index=False)
                print(f"Comments analysis complete! Saved to {comments_csv_filename}")
                print(f"Total comments scraped: {len(all_comments)}")
                
                # Upload to Google Sheets
                if sheets_client and spreadsheet_key:
                    upload_to_sheets(sheets_client, comments_df, "LinkedIn Comments", spreadsheet_key)
                    print(f"Comments data uploaded to Google Sheets with ID: {spreadsheet_key}")
            else:
                print("No comments were found or scraped.")
        else:
            print("Could not find any LinkedIn posts. Please check your login status and try again.")
            
    except Exception as e:
        print(f"Error in main function: {str(e)}")
        
    print("LinkedIn scraper completed.")

if __name__ == "__main__":
    main()