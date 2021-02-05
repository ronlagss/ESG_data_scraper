# import selenium modules
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import pandas as pd
import os
from datapackage import Package
import boto3


session = boto3.Session(
    aws_access_key_id='', #EXAMPLE KEY ID
    aws_secret_access_key='', #EXAMPLE KEY 
)
dynamodb = session.resource('dynamodb')
dynamodb_table = dynamodb.Table('yahoo-finance')

# Create a headless instance of chrome
chrome_options = Options()
chrome_options.add_argument("--headless")

# add your chromedriver path here
PATH = str(os.getcwd()) + '/chromedriver'
driver = webdriver.Chrome(PATH, options=chrome_options)

# get package from datahub. this is the list of s&p 500 companies.
package = Package('https://datahub.io/core/s-and-p-500-companies/datapackage.json')

# print list of all resources:
print(package.resource_names)

cols = ['Ticker','Name','Industry','Total ESG Risk Score','Environment Risk Score','Social Risk Score','Governance Risk Score']
data = []
df = pd.DataFrame()

# print processed tabular data (if exists any)
for resource in package.resources:
    if resource.descriptor['datahub']['type'] == 'derived/csv':
        # company var contains a 2D array of company data, in each list item contains ticker, name, and industry.
        company = resource.read()

        print(len(company))
        for i in range(0, len(company)):
            # use 0 for ticker, 1 for name, and 2 for industry
            # get data
            print(str(i) + '>>>>>>>>>>' + company[i][0])
            driver.get("https://finance.yahoo.com/quote/" + company[i][0] + "/sustainability")
            try:
                name = driver.find_element_by_xpath('//*[@id="quote-header-info"]/div[2]/div[1]/div[1]/h1').text
                sym1 = name.split("(")
                sym2 = sym1[1].split(")")
                sym = sym2[0]
            except:
                name = ''
                sym = ''
            
            try:
                total = driver.find_element_by_xpath('//*[@id="Col1-0-Sustainability-Proxy"]/section/div[1]/div/div[1]/div/div[2]/div[1]').text
                env = driver.find_element_by_xpath('//*[@id="Col1-0-Sustainability-Proxy"]/section/div[1]/div/div[2]/div/div[2]/div[1]').text
                soc = driver.find_element_by_xpath('//*[@id="Col1-0-Sustainability-Proxy"]/section/div[1]/div/div[3]/div/div[2]/div[1]').text
                gov = driver.find_element_by_xpath('//*[@id="Col1-0-Sustainability-Proxy"]/section/div[1]/div/div[4]/div/div[2]/div[1]').text
            except:
                total = ''
                env = ''
                soc = ''
                gov = ''

            values = [sym, name, company[i][2], total, env, soc, gov]
            print(values)
            zipped = zip(cols, values)
            zip_dict = dict(zipped)
            
            dynamodb_table.put_item(
                        Item = {
                            'id': company[i][0],
                            'ticker': sym,
                            'name': name,
                            'industry': company[i][2],
                            'total_rist_score': total,
                            'env_risk_score': env,
                            'social_risk_score': soc,
                            'goverance_risk_score': gov
                        }
                    )
            data.append(zip_dict)

df = df.append(data, True)
print(df)
df.to_csv('output.csv', header=cols, index=False)

'''
df1 = pd.read_csv('output.csv')
if df1.empty:
    df.to_csv('output.csv', header=list, index=False)
else:
    df3 = pd.DataFrame([list1], columns=list)
    df4 = pd.concat([df1, df3])
    df4.to_csv('output.csv', header=list, index=False)
'''

# Quit
driver.quit()



