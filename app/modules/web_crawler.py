import os
import glob
import time
from selenium import webdriver
from selenium.webdriver.firefox.options import Options

data_directory = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'data/')


def web_crawler(month, year, directory):
    # This function crawls the faa flight website and downloads a zip file
    # Create Firefox profile
    profile = webdriver.FirefoxProfile()

    # Preference set for MIME type to ignore help message and specify download location
    profile.set_preference('browser.helperApps.neverAsk.saveToDisk', 'application/x-zip-compressed')
    profile.set_preference('browser.download.folderList', 2)
    profile.set_preference('browser.download.dir', directory)

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

    condition = True
    while condition:
        if glob.glob(directory + '*.part'):
            time.sleep(10)
        else:
            if glob.glob(directory + '*.zip'):
                browser.close()
                condition = False

    return None

