from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import bson
import sys
import json
import time
import urllib.request
from urllib.error import HTTPError

USER_AGENT = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.47 Safari/537.36'
currencies = {"euro": "€", "dollar": "$", "pound": "£"}

def parseURL(url):
    if not url.startswith("http://") and not url.startswith("https://"):
        url = "https://" + url
        return url

def connectToDB(MongoURI):
    # Create a new client and connect to the server
    client = MongoClient(MongoURI, server_api=ServerApi('1'))
    # Send a ping to confirm a successful connection
    try:
        client.admin.command('ping')
        print("Pinged your deployment. You successfully connected to MongoDB!")
        return client['test']
    except Exception as e:
        print(e)

def getOrCreateBrand(brandName, client, url):
    brands = client['brands']
    brand = brands.find_one({"name": brandName})
    if brand:
        return brand['_id']
    else:
        brand = {
            "name": brandName,
            "followers": [],
            "banner": "",
            "profile_picture": "",
            "url": url
        }
        brands.insert_one(brand)
        print("Created brand")
        getOrCreateBrand(brandName, client, url)

def getProducts(url):
    index = 1
    products = []
    valid = True
    while valid:
        try:
            req = urllib.request.Request(
                url + '/products.json?page={}'.format(index),
                data=None,
                headers={
                    'User-Agent': USER_AGENT
                }
            )
            try:
                data = urllib.request.urlopen(req).read()
                pageProducts = json.loads(data.decode())['products']
                if not pageProducts: 
                    valid = False
                    break
                products += pageProducts
                index += 1
                
            except HTTPError:
                print('Blocked! Sleeping...')
                time.sleep(180)
                print('Retrying')
        except:
            valid = False
            print("invalid")
    return products

def getRelevantProducts(products, storeName, storeID, storeCurrency, url):
    relevantProducts = []
    for product in products:
        variants = product['variants']
        in_stock = False
        images = []
        for variant in variants:
            if variant['available'] == True:
                in_stock = True
            price = variant['price']
        for image in product['images']:
            images.append(image['src'])
        relevantProduct = {
            "name": product['title'],
            "store_name": storeName,
            "store_ID": bson.ObjectId(storeID),
            "product_type": product['product_type'],
            "product_url": url + '/products/' + product['handle'],
            "description": product['body_html'],
            "published_at": product['published_at'],
            "in_stock": in_stock,
            "price": currencies[storeCurrency] + price,
            "images": images,
            "likes": []
        }
        relevantProducts.append(relevantProduct)
    return relevantProducts

def insertProducts(products, client):
    posts = client['posts']
    try:posts.insert_many(products)
    except:print("Error inserting")

if __name__ == '__main__':
    if len(sys.argv) != 5:
        print("This program is expected to be run with exactly 4 arguments: URL, Store Name, Currency of store (euro, dollar, pound), mongo URI")
    else:
        url = parseURL(sys.argv[1])
        storeName = sys.argv[2]
        currency = sys.argv[3]
        mongo_uri = sys.argv[4]
        client = connectToDB(mongo_uri)
        brandId = getOrCreateBrand(storeName, client, url)
        products = getProducts(url)
        relevantProducts = getRelevantProducts(products, storeName, brandId, currency, url)
        insertProducts(relevantProducts, client)