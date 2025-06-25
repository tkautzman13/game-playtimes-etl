from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.remote.webdriver import WebDriver
import time
from pathlib import Path
from src.utils import create_date_folder_path, get_logger
from datetime import datetime

def setup_chrome_driver() -> WebDriver:
    """
    Set up and return a Chrome WebDriver instance.
    
    Returns:
        WebDriver: Configured Chrome WebDriver instance
    """
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))
    return driver


def login_to_exophase(driver: WebDriver, username: str, password: str, login_url: str = "https://www.exophase.com/login/") -> None:
    """
    Log into Exophase with provided credentials.
    
    Parameters:
        driver (WebDriver): WebDriver instance
        username (str): Exophase username
        password (str): Exophase password
        login_url (str): URL for Exophase login page
    
    Returns:
        None
    """
    driver.get(login_url)
    
    # Fill in login form
    driver.find_element(By.NAME, "username").send_keys(username)
    driver.find_element(By.NAME, "password").send_keys(password + Keys.RETURN)
    
    time.sleep(2)


def run_profile_scan(
        driver: WebDriver, 
        url: str = "https://www.exophase.com/nintendo/user/b2e58f38785ac9a0/",
        scan_wait_time: int = 60) -> None:
    """
    Navigate to profile URL, run a profile scan, and wait for completion.
    
    Parameters:
        driver (WebDriver): WebDriver instance
        url (str): Profile URL to navigate to
        scan_wait_time (int): Time to wait for scan completion in seconds
    
    Returns:
        None
    """
    # Navigate to URL
    driver.get(url)

    wait = WebDriverWait(driver, 10)

    # Click on the user dropdown toggle
    dropdown_toggle = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, ".top-panel .dropdown-toggle")))
    dropdown_toggle.click()

    # Click on "Run Profile Scan"
    run_scan = wait.until(EC.element_to_be_clickable((By.XPATH, "//a[text()='Run Profile Scan']")))
    run_scan.click()

    time.sleep(scan_wait_time)

    # Refresh the page
    driver.refresh()

    time.sleep(5)


def save_page_source(driver: WebDriver, folder_path: Path, filename: str) -> None:
    """
    Save the current page source to a file.
    
    Parameters:
        driver (WebDriver): WebDriver instance
        output_file_path (str): Path where to save the HTML file
    
    Returns:
        None
    """
    # Get current date
    current_date = datetime.now()
    current_datetime = current_date.strftime("%Y-%m-%d_%H-%M-%S")

    filename_dt = f"{current_datetime}_{filename}"

    file_path = folder_path / filename_dt

    with open(f'{file_path}', "w", encoding="utf-8") as f:
        f.write(driver.page_source)
    print(f"Page source saved to: {file_path}")


def scrape_switch_playtimes(
        username: str, 
        password: str,
        output_path: str,
        output_filename: str = 'switch_playtime.html') -> None:
    """
    Scrape Exophase Nintendo Switch playtimes using Selenium.
    This function logs in, runs a profile scan, and saves the profile page HTML.
    
    Parameters:
        username (str): Exophase username for login
        password (str): Exophase password for login
        output_file_path (str): Path where to save the HTML file
    
    Returns:
        None
    """
    driver = None
    try:
        # Setup driver
        driver = setup_chrome_driver()
        
        # Login
        login_to_exophase(driver, username, password)
        
        # Run profile scan
        run_profile_scan(driver)

        # Create date folder path
        date_folder = create_date_folder_path(output_path)
        
        # Save page source
        save_page_source(driver, folder_path=date_folder, filename=output_filename)
        
    finally:
        # Ensure driver is closed even if an error occurs
        if driver:
            driver.quit()