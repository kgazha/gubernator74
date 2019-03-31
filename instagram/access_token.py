import requests
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
import urllib.parse as urlparse
import pickle


client_id = ""
client_secret = ""
username = ""
password = ""
redirect_uri = "https://yandex.ru/"


def get_authorization_code():
    authorization_url = "https://api.instagram.com/oauth/authorize/?client_id=" + client_id \
                        + "&redirect_uri=" + redirect_uri + "&response_type=code"
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument('--headless')
    chrome_options.add_argument('--ignore-certificate-errors')
    chrome_options.add_argument('--enable-features=NetworkService')
    driver = webdriver.Chrome(chrome_options=chrome_options)
    driver.set_window_position(0, 0)
    driver.set_window_size(1024, 768)
    driver.get(authorization_url)
    username_element = driver.find_elements(By.XPATH, '//input[@name="username"]')[0]
    username_element.send_keys(username)
    password_element = driver.find_elements(By.XPATH, '//input[@name="password"]')[0]
    password_element.send_keys(password)
    driver.find_elements(By.XPATH, '//button[@type="submit"]')[0].click()
    wait = WebDriverWait(driver, 10)
    wait.until(lambda _driver: redirect_uri in _driver.current_url)
    url = driver.current_url
    parsed_url = urlparse.urlparse(url)
    driver.quit()
    return urlparse.parse_qs(parsed_url.query)['code'][0]


code = get_authorization_code()
r = requests.post("https://api.instagram.com/oauth/access_token",
                  data={"client_id": client_id, "client_secret": client_secret,
                        "grant_type": "authorization_code", "redirect_uri": redirect_uri,
                        "code": code})

response = r.json()
access_token = response['access_token']
with open('access_token.pickle', 'wb') as file:
    file.write(pickle.dumps(access_token))
