from datetime import date
import os
import csv

from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options

#folder constants
HOME = os.path.expanduser('~')
DATALAKE_ROOT_FOLDER = HOME + "/datalake/"

def fetch_trending_movies_boxoffice(**kwargs):
    chrome_options = Options() 
    chrome_options.add_experimental_option("detach", True)

    driver = webdriver.Chrome()
    driver.get("https://www.boxofficemojo.com")
    elem = driver.find_element(by=By.XPATH, value='//*[@id="a-page"]/main/div/div/div[2]/div[1]/div[1]/div/div[5]/a')

    #Insert data to datalake
    header = ['rank', 'title', 'distributor', 'total_revenue', 'date_total_revenue', 'theatre_count']
    data=[]

    for i in range(1,5):
        date_total_revenue = elem.get_attribute('href').split('/')[4]
        #data_archived_date = date.today()

        elem.click()
        rows = len(driver.find_elements(by=By.XPATH, value='//*[@id="table"]/div/table[2]/tbody/tr'))

        # Printing the data of the table
        for r in range(2, rows+1):
            try:
                rank = driver.find_element(by=By.XPATH, value='//*[@id="table"]/div/table[2]/tbody/tr['+str(r)+']/td[1]').text
                title = driver.find_element(by=By.XPATH, value='//*[@id="table"]/div/table[2]/tbody/tr['+str(r)+']/td[3]').text
                theatre_count = driver.find_element(by=By.XPATH, value='//*[@id="table"]/div/table[2]/tbody/tr['+str(r)+']/td[7]').text
                total_revenue = driver.find_element(by=By.XPATH, value='//*[@id="table"]/div/table[2]/tbody/tr['+str(r)+']/td[9]').text
                distributor = driver.find_element(by=By.XPATH, value='//*[@id="table"]/div/table[2]/tbody/tr['+str(r)+']/td[11]/a').text
                data.append([rank, title, distributor, total_revenue, date_total_revenue, theatre_count])
            except:
                next
        
        #move to previous day
        elem = driver.find_element(by=By.XPATH, value='//*[@id="a-page"]/main/div/div/div[3]/div[1]/a[1]')
    
    #Insert data to datalake
    current_day = date.today().strftime("%Y%m%d")
    TARGET_PATH = DATALAKE_ROOT_FOLDER + "raw/boxoffice/box_office_movies/" + current_day + "/"
    if not os.path.exists(TARGET_PATH):
        os.makedirs(TARGET_PATH)
    print("Writing here: ", TARGET_PATH)

    with open(TARGET_PATH + "boxoffice.csv", 'w', encoding='UTF8', newline='') as f:
        writer = csv.writer(f)
        # write the header
        writer.writerow(header)
        # write multiple rows
        writer.writerows(data)