from selenium import webdriver
from selenium.webdriver.firefox.options import Options


def web_crawler(month, year):
    # This function crawls the faa flight website and downloads a zip file
    # Create Firefox profile
    profile = webdriver.FirefoxProfile()

    # Preference set for MIME type to ignore help message when browser downloads file
    profile.set_preference('browser.helperApps.neverAsk.saveToDisk', 'application/x-zip-compressed')

    # Creates Options Class
    options = Options()
    options.headless = True  # Hides the browser from view

    browser = webdriver.Firefox(firefox_profile=profile, options=options)
    browser.get('https://transtats.bts.gov/DL_SelectFields.asp?Table_ID=236&DB_Short_Name=On-Time')  # Loads website
    browser.find_element_by_id('DownloadZip').click()  # Clicks checkbox for zip file

    # Criteria to download
    browser.find_element_by_xpath("//select[@name='FREQUENCY']/option[text()='{0}']".format(month)).click()
    browser.find_element_by_xpath("//select[@name='XYEAR']/option[text()='{0}']".format(year)).click()

    # Click download button
    browser.find_element_by_name('Download2').click()

    return None
