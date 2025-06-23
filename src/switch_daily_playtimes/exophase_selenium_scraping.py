from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time

driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))

driver.get("https://www.exophase.com/login/")

# Fill in login form
driver.find_element(By.NAME, "username").send_keys("")
driver.find_element(By.NAME, "password").send_keys("" + Keys.RETURN)

time.sleep(2)

# Now go to your profile
profile_url = "https://www.exophase.com/nintendo/user/b2e58f38785ac9a0/"
driver.get(profile_url)

time.sleep(2)

wait = WebDriverWait(driver, 10)

# Click on the user dropdown toggle
dropdown_toggle = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, ".top-panel .dropdown-toggle")))
dropdown_toggle.click()

# Click on "Run Profile Scan"
run_scan = wait.until(EC.element_to_be_clickable((By.XPATH, "//a[text()='Run Profile Scan']")))
run_scan.click()

# Wait 30 seconds
time.sleep(30)

# Refresh the page
driver.refresh()

# Wait 5 seconds
time.sleep(5)

with open("C:/Users/tk231/Desktop/Exophase Scraping Testing/profile.html", "w", encoding="utf-8") as f:
    f.write(driver.page_source)

driver.quit()