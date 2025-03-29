import time
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
import logging
from dotenv import dotenv_values


# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


# Search Keyword
SEARCH_KEYWORD = "Artificial Intelligence"

def setup_driver():
    """Set up and return a configured ChromeDriver instance"""
    logging.info("Setting up Chrome driver")
    try:
        options = webdriver.ChromeOptions()
        options.add_argument("--start-maximized")
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option("useAutomationExtension", False)
        
        # Block Notifications
        prefs = {
            "profile.default_content_setting_values.notifications": 2,
            "profile.default_content_setting_values.popups": 2,
        }
        options.add_experimental_option("prefs", prefs)

        logging.info("Installing ChromeDriver using ChromeDriverManager")
        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
        logging.info("Chrome driver setup completed successfully")
        return driver
    except Exception as e:
        logging.error(f"Failed to setup Chrome driver: {str(e)}")
        raise

def google_login(driver, google_email, google_password):
    """Login to LinkedIn using Google credentials"""
    logging.info("Starting Google login process")
    initial_window = driver.current_window_handle

    try:
        # Locate and switch to iframe within auth flow manager
        logging.info("Switching to iframe for Google login")
        iframe = WebDriverWait(driver, 20).until(
                        EC.presence_of_element_located((By.TAG_NAME, "iframe")))
        driver.switch_to.frame(iframe)

        # Now find and click the Google button
        logging.info("Clicking 'Continue with Google' button")
        google_button = WebDriverWait(driver, 20).until(
                    EC.element_to_be_clickable((By.XPATH, "//span[contains(text(), 'Continue with Google')]")))
        google_button.click()

        # Switch to the new window (Google login)
        logging.info("Switching to Google login window")
        driver.switch_to.window(driver.window_handles[1])

        logging.info("Entering Google email")
        email_field = WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.XPATH, "//input[@type='email']"))
         )
        email_field.send_keys(google_email)
        email_field.send_keys(Keys.RETURN)
        logging.info("Submitted email")

        time.sleep(4)

        logging.info("Entering Google password")
        password_field = WebDriverWait(driver, 20).until(
                EC.presence_of_element_located((By.NAME, "Passwd"))
            )
        password_field.send_keys(google_password)
        password_field.send_keys(Keys.RETURN)
        logging.info("Submitted password")
        time.sleep(5)

        # Switch back to the original window
        driver.switch_to.window(initial_window)
        logging.info("Successfully logged into website using Google")
        return 'success'
    except Exception as e:
        logging.error(f"Google login failed: {str(e)}")
        return 'failed'

def search_keyword(driver):
    """Search for the specified keyword"""
    logging.info(f"Starting search for keyword: {SEARCH_KEYWORD}")
    try:
        logging.info("Locating search box")
        search_box = WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.XPATH, "//input[contains(@placeholder,'Search')]"))
        )
        search_box.send_keys(SEARCH_KEYWORD)
        search_box.send_keys(Keys.RETURN)
        logging.info("Submitted search query")
        time.sleep(3)

        # Click on "Posts" tab
        logging.info("Clicking on Posts tab")
        posts_tab = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, "/html/body/div[6]/div[3]/div[2]/div/div[1]/main/div/div/div[2]/div/div[2]")))
        posts_tab.click()
        logging.info("Posts tab clicked")
        time.sleep(3)
        return True
    except Exception as e:
        logging.error(f"Search failed: {str(e)}")
        return False

def scrape_posts(driver):
    """
    Scrape LinkedIn posts including author details from separate containers, content, and comments.
    
    Args:
        driver: Selenium WebDriver instance
        
    Returns:
        List of dictionaries containing post data (author details, content, comments)
    """
    
    # Initialize logging and data storage
    logging.info("Starting comprehensive post scraping process")
    posts_data = []
    last_height = driver.execute_script("return document.body.scrollHeight")
    
    try:
        # Scroll loop - attempts to load more content by scrolling
        for scroll_attempt in range(3):  # Maximum of 3 scroll attempts
            logging.info(f"Scrolling attempt {scroll_attempt + 1}")
            
            # Wait for posts to load and locate all post content elements
            post_content_elements = WebDriverWait(driver, 15).until(
                EC.presence_of_all_elements_located((By.XPATH, 
                    "//div[contains(@class, 'update-components-text relative update-components-update-v2__commentary')]"))
            )
            
            # Locate all author containers separately
            author_containers = driver.find_elements(By.XPATH,
                "//div[contains(@class, 'update-components-actor')]")
            
            # Verify we have matching counts
            if len(post_content_elements) != len(author_containers):
                logging.warning(f"Mismatch found: {len(post_content_elements)} posts vs {len(author_containers)} authors")
            
            # Process each post with its corresponding author container
            for index, (content_div, author_div) in enumerate(zip(post_content_elements, author_containers)):
                try:
                    post_data = {}
                    
                    # --- AUTHOR DETAILS EXTRACTION ---
                    try:
                        # Extract profile URL if available
                        profile_link = author_div.find_element(
                            By.XPATH, ".//a[contains(@class, 'update-components-actor__meta-link')]"
                        ).get_attribute("href")
                    except NoSuchElementException:
                        profile_link = "N/A"
                    
                    # Extract author name
                    try:
                        author_name = author_div.find_element(
                            By.XPATH, ".//span[contains(@class, 'update-components-actor__name')]"
                        ).text.strip()
                    except NoSuchElementException:
                        author_name = "N/A"
                    
                    # --- POST CONTENT EXTRACTION ---
                    try:
                        # Get all text content including nested elements
                        full_text = content_div.get_attribute("textContent")
                        post_text = ' '.join(full_text.split()).strip()
                    except Exception as e:
                        post_text = "N/A"
                        logging.warning(f"Failed to extract post text: {str(e)}")
                    
                    # --- COMMENT EXTRACTION ---
                    comments = []
                    try:
                        # Find the parent container that holds both post and comments
                        parent_container = content_div.find_element(
                            By.XPATH, "./ancestor::div[contains(@class, 'update-components-update-v2')]")
                        
                        # Click comment button
                        comment_button = parent_container.find_element(
                            By.XPATH, ".//button[contains(@aria-label, 'Comment')]")
                        driver.execute_script("arguments[0].click();", comment_button)
                        time.sleep(1.5)
                        
                        # Extract comments
                        comment_elements = parent_container.find_elements(
                            By.XPATH, ".//div[contains(@class, 'comments-comment-item')]")
                        
                        for comment in comment_elements:
                            try:
                                comment_text = comment.find_element(
                                    By.XPATH, ".//div[contains(@class, 'comment__text-content')]"
                                ).get_attribute("textContent").strip()
                                if comment_text:
                                    comments.append(' '.join(comment_text.split()))
                            except:
                                continue
                    except Exception as e:
                        logging.debug(f"Comment extraction failed: {str(e)}")
                    
                    # Store the collected data
                    posts_data.append({
                        "Author": {
                            "name": author_name,
                            "profile_url": profile_link
                        },
                        "Post Content": post_text,
                        "Comments": comments if comments else "No comments"
                    })
                    
                except Exception as e:
                    logging.error(f"Error processing post {index + 1}: {str(e)}")
                    continue

            # --- SCROLLING MECHANISM ---
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(2.5)  # Adjusted wait time
            
            new_height = driver.execute_script("return document.body.scrollHeight")
            if new_height == last_height:
                logging.info("Reached end of page, stopping scrolling")
                break
            last_height = new_height

        logging.info(f"Successfully scraped {len(posts_data)} posts with author details")
        return posts_data
    
    except Exception as e:
        logging.error(f"Error during post scraping: {str(e)}", exc_info=True)
        return posts_data
    

    
def main():
    """Main execution function"""
    logging.info("Starting LinkedIn scraper")
    try:
        # Load credentials
        logging.info("Loading credentials from .env file")
        config = dotenv_values(".env")
        google_email = config.get("EMAIL")
        google_password = config.get("PASSWORD")
        
        if not google_email or not google_password:
            error_msg = "Google credentials not found in .env file"
            logging.error(error_msg)
            raise ValueError(error_msg)
        else:
            logging.info("Credentials loaded successfully")
        
        # Initialize driver
        driver = setup_driver()
        
        logging.info("Navigating to LinkedIn homepage")
        driver.get("https://www.linkedin.com")
        
        # Perform Google login
        logging.info("Attempting Google login")
        login_result = google_login(driver, google_email, google_password)
        if login_result != 'success':
            raise Exception("Google login failed")
        
        # Search and scrape
        logging.info("Starting search and scrape process")
        if search_keyword(driver):
            posts = scrape_posts(driver)
            if posts:
                logging.info(f"Saving {len(posts)} posts to CSV")
                df = pd.DataFrame(posts)
                df.to_csv("linkedin_posts_with_comments.csv", index=False)
                logging.info("Data successfully saved to linkedin_posts_with_comments.csv")
            else:
                logging.warning("No posts were scraped")
        else:
            logging.error("Search keyword failed")
        
    except Exception as e:
        logging.error(f"Script failed with error: {str(e)}", exc_info=True)
    finally:
        if 'driver' in locals():
            logging.info("Quitting driver")
            driver.quit()
        logging.info("Scraping process completed")

if __name__ == "__main__":
    main()