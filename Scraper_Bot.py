import asyncio, sqlite3, os, time, datetime, json
from random import choice
from threading import Thread
from requests_html import HTML, HTMLSession
import telegram, aiohttp
from telegram.ext import Updater, CommandHandler
from scraper_api import ScraperAPIClient


class AmazonScraper(object):
    """
    docstring for AmazonScraper Class to Scrape Amazon.com.tr
    """
    def __init__(self, API_KEY):
        self.sleep_time = 5 # Minutes
        self.API_KEY = API_KEY  # ScraperAPI Proxies
    
        self.bot_1 = telegram.Bot(<token 1>) # Bot 1
        self.bot_2 = telegram.Bot(<token 2>) # Bot 2
        self.bot_3 = telegram.Bot(<token 3>) # Bot 3
        self.bot_4 = telegram.Bot(<token 4>) # Bot 4
        
        self.ADMIN_LIST = [<int>]                           # Admin Chat ID
        self.test_group_chat_id  = <int>                    # Group Chat ID
        
        self.db_name = os.getcwd() + './data/Amazon Database.db'                  # DataBase Name
        self.txt_file = open((os.getcwd() + './data/categories links.txt'),'r')  # category links text file
        self.settings = json.load(open((os.getcwd() + './data/settings.json'), 'r'))
        
        # Defaults
        self.running_first_time       = True
        self.running_first_time_count = 0
        
        self.discount_rate = self.settings['discount']
        self.raise_rate    = self.settings['raise']
        self.log_condition = self.settings['log']
        self.new_product   = self.settings['new']

        self.products = {}
        self.products_db = {}
        self.product_count = 0
        self.duplicate_count = 0

        self.create_database()
        self.load_database()

    def create_database(self):
        con = sqlite3.connect(self.db_name)
        con.execute('''CREATE TABLE IF NOT EXISTS Products (
                            Product_ASIN TEXT PRIMARY KEY NOT NULL UNIQUE,
                            Product_Name TEXT NOT NULL,
                            Product_Price REAL,
                            Product_Link TEXT,
                            update_time TIMESTAMP);'''
                    )
        con.close()

    def load_database(self):
        con = sqlite3.connect(self.db_name)
        temp_data = con.execute(""" SELECT * FROM Products """)
        temp_data = temp_data.fetchall()
        for row in temp_data:
            self.products_db[row[0]] = {
                'Product_ASIN' : row[0], 'Product_Name':row[1], 'Product_Price':row[2],
                'Product_Link':row[3],'update_time':row[4]
            }
        con.close()
    
    def send_message_to_group(self, chat_id, text):
        while True:
            bot = choice([self.bot_1, self.bot_2, self.bot_3, self.bot_4])
            try : bot.send_message(chat_id=chat_id, text=text); break
            except Exception as e: time.sleep(1)
    
    def send_message_to_bot(self, chat_id, text):
        while True:
            try : self.bot_1.send_message(chat_id=chat_id, text=text) ; break
            except Exception as e : time.sleep(1)
    
    async def parse(self, html):
        html = HTML(html=html)
        for prod in html.find('div[data-component-type="s-search-result"]'):
            item = {}
            product_price = prod.xpath('.//span[@class="a-offscreen"]/text()',first=True)
            ASIN = prod.xpath('.//@data-asin',first=True)
            if product_price == None or ASIN == None : continue
            item['Product_ASIN'] = ASIN
            item['Product_Name']  = prod.xpath('.//span[@class="a-size-base-plus a-color-base a-text-normal"]/text()',first=True).strip().replace('\xa0', ' ')
            price = float(product_price.replace('\xa0', '').replace('TL','').replace('.','').replace(',','.'))
            item['Product_Price'] = round(price,2)
            item['Product_Link']  = 'https://www.amazon.com.tr/dp/' + ASIN
            item['update_time'] = str(datetime.datetime.now())
            
            if ASIN not in self.products:
                self.products[ASIN] = item
                self.product_count += 1
            else:
                self.duplicate_count += 1

    async def fetch_eith_sem(self, sem, cat_url, asession):
        async with sem:
            async with asession.get(cat_url) as response:
                if response.status in range(200, 300):
                    print(response.url)
                    html = await response.text()
                    await self.parse(html)
                elif response.status == 429:
                    print(f'Error {response.status} too many requests,,, Sleep 1 minute ...')
                    await asyncio.sleep(60)

    async def main(self,links ,semaphore=8):
        try:
            s = HTMLSession()
            async with aiohttp.ClientSession() as asession:
                sem = asyncio.Semaphore(semaphore)
                urls = []
                for cat_link in links:
                    API_link = f'http://api.scraperapi.com/?api_key={self.API_KEY}&url=' + cat_link
                    resp = s.get(API_link)
                    pages = resp.html.xpath('//li[@class="a-disabled" and contains(text(),"")][last()]/text()', first=True)
                    if not pages : pages = 1
                    urls_1 = [API_link + f'&page={p}' for p in range(1,(int(pages)+1))]
                    for url_1 in urls_1: urls.append(url_1)
                tasks = [asyncio.ensure_future(self.fetch_eith_sem(sem, url, asession)) for url in urls]
                await asyncio.gather(*tasks)
            await asession.close()
        except aiohttp.ClientConnectionError as e:
            print('Error handelded')

    def Crawling(self):
        categories_links = [link.strip() for link in self.txt_file.readlines()]
        while True:
            del self.products
            self.products = {}
            self.product_count = 0
            self.duplicate_count = 0
            start_time = time.time()
            asyncio.run(self.main(categories_links))
            
            if not self.running_first_time:
                con = sqlite3.connect(self.db_name)
                for product in self.products.values():
                    # Check new products
                    if product['Product_ASIN'] not in self.products_db:
                        sql_insert = '''INSERT OR IGNORE INTO Products (Product_ASIN, Product_Name, Product_Price, Product_Link, update_time) VALUES (?,?,?,?,?);'''
                        con.execute(sql_insert, 
                            (str(product.get('Product_ASIN')), str(product.get('Product_Name')),
                            str(product.get('Product_Price')), str(product.get('Product_Link')),
                            str(product.get('update_time')),)
                            )
                        self.products_db[product['Product_ASIN']] = product
                        text = f"""New Product :\n{product['Product_Name']} Bu ürün yeni eklendi!\nFiyat: {product['Product_Price']}\nURL :\n{product['Product_Link']}\n"""  # Yeni eklenen ürün mesajı
                        if self.new_product :
                            self.send_message_to_group(self.test_group_chat_id, text)
                            print(text)
                        
                    else :
                        # Update old products prices
                        old_price = self.products_db[product['Product_ASIN']]['Product_Price']
                        if old_price != product['Product_Price']:
                            # check Discount Case
                            if old_price > product['Product_Price'] and (old_price - product['Product_Price']) / old_price > ( self.discount_rate * 0.01):
                                self.products_db[product['Product_ASIN']]['Product_Price'] = product['Product_Price']
                                text = f"""%{abs(int(((product['Product_Price'] - old_price) / old_price) * 100))} Discount\n{product['Product_Name']}\n{old_price} TL >> {product['Product_Price']} TL\nURL :\n{product['Product_Link']}\n"""
                                self.send_message_to_group(self.test_group_chat_id, text)
                                print(text)
                            
                            # check Raise Case
                            elif old_price < product['Product_Price'] and ((product['Product_Price'] - old_price) / old_price) > (self.raise_rate * 0.01):
                                    text = f"""%{abs(int(((product['Product_Price'] - old_price) / old_price) * 100))} Raise\n{product['Product_Name']}\n{old_price} TL >> {product['Product_Price']} TL\nURL :\n{product['Product_Link']}\n"""
                                    self.send_message_to_group(self.test_group_chat_id, text)
                                    print(text)
                            con.execute('UPDATE Products SET Product_Price = ?, update_time = ? WHERE Product_ASIN = ?;',
                                        (str(product['Product_Price']), datetime.datetime.now(), str(product['Product_ASIN']))
                                        )
                            self.products_db[product['Product_ASIN']]['Product_Price'] = product['Product_Price']
                con.commit()
                con.close()

            elif self.running_first_time:
                con = sqlite3.connect(self.db_name)
                for product in self.products.values():
                    if product['Product_ASIN'] not in self.products_db:
                        sql_insert = '''INSERT OR IGNORE INTO Products (Product_ASIN, Product_Name, Product_Price, Product_Link, update_time) VALUES (?,?,?,?,?);'''
                        con.execute(sql_insert, 
                            (str(product.get('Product_ASIN')), str(product.get('Product_Name')),
                            str(product.get('Product_Price')), str(product.get('Product_Link')),
                            str(product.get('update_time')),)
                            )
                        self.products_db[product['Product_ASIN']] = product
                con.commit()
                con.close()
                self.running_first_time_count += 1
                if self.running_first_time_count == 2:
                    self.running_first_time = False
                    text = f"İlk defa çalışma işlemi tamamlandı, Ürün Sayısı: {len(self.products_db)}, Duplicate Sayısı: {self.duplicate_count}"
                    print(text)
                    self.send_message_to_group(self.test_group_chat_id, text)
            
            text = f"Geçen Süre: {(time.time() - start_time):.1f} saniye, {self.product_count} ürünler"
            if self.log_condition:
                self.send_message_to_bot(self.test_group_chat_id, text)
                print(text)
                
            print(f'Spleeping {self.sleep_time} minutes and continue')
            time.sleep(self.sleep_time*60)
    
    def scraping_loop(self):        
        while True:
            print('Yeni ürünler ve güncellemeler aranıyor...')
            try : self.Crawling()
            except Exception as e:
                print(f"Hata aldı ve 10 saniye uyuyacak hata: {e}")
                time.sleep(10)
    
    def help_(self, update, context):
        chat_id = update.effective_chat.id
        if chat_id in self.ADMIN_LIST:
            self.send_message_to_bot(chat_id, "Aşka Yürek Gerek Anlasana")

    def update_discount(self, update, context):
        discount_rate = self.discount_rate
        chat_id = update.effective_chat.id
        if chat_id in self.ADMIN_LIST:
            try : command, new_discount_rate = (update.message.text).split(" ")
            except ValueError:
                self.send_message_to_bot(chat_id, text="Girdi Yanlış Formatta Girildi!")
                return 0
            if new_discount_rate.isnumeric:
                try :   new_discount_rate = int(new_discount_rate)
                except ValueError:
                    self.send_message_to_bot( self.bot_1_cht_id , text="İndirim Oranı Yanlış Formatta Girildi.")
                    return 0

                if new_discount_rate == 0:
                    self.send_message_to_bot(chat_id, text="İndirim Oranı 0 olamaz.")
                    return 0
            else:
                self.send_message_to_bot(chat_id, text="İndirim Oranı Yanlış Formatta Girildi.")
                return 0
            text=f"{discount_rate} Olan İndirim Oranı {new_discount_rate} Oranına Güncellenmiştir."
            self.send_message_to_bot(chat_id , text)
            self.settings['discount'] = new_discount_rate
            json.dump(self.settings, open((os.getcwd()+'./data/settings.json'), 'w'))

    def update_raise(self, update, context):
        raise_rate = self.raise_rate
        chat_id = update.effective_chat.id
        if chat_id in self.ADMIN_LIST:
            try : command, new_raise_rate = (update.message.text).split(" ")
            except ValueError:
                self.send_message_to_bot(chat_id, text="Girdi Yanlış Formatta Girildi!")
                return 0
            if new_raise_rate.isnumeric:
                try:  new_raise_rate = int(new_raise_rate)
                except ValueError:
                    self.send_message_to_bot(chat_id, text="zam Oranı Yanlış Formatta Girildi.")
                    return 0
                if new_raise_rate == 0:
                    self.send_message_to_bot(chat_id, text="zam Oranı 0 olamaz.")
                    return 0
            else:
                self.send_message_to_bot(chat_id, text="zam Oranı Yanlış Formatta Girildi.")
                return 0
            text=f"{raise_rate} Olan zam Oranı {new_raise_rate} Oranına Güncellenmiştir."
            self.send_message_to_bot(chat_id, text)
            self.raise_rate = new_raise_rate
            self.settings['raise'] = new_raise_rate
            json.dump(self.settings, open((os.getcwd()+'./data/settings.json'), 'w'))

    def log_updater(self, update, context):
        chat_id = update.effective_chat.id
        if chat_id in self.ADMIN_LIST:
            try : command, new_log_type = (update.message.text).split(" ")
            except (ValueError, AttributeError):
                self.send_message_to_bot(chat_id, text="Girdi Yanlış Formatta Girildi!")
                return 0

            if new_log_type.lower() == "on":
                self.send_message_to_bot(chat_id, text="Log değiştirildi. ON")
                self.settings['log'] = True
                json.dump(self.settings, open((os.getcwd()+'./data/settings.json'), 'w'))

            elif new_log_type.lower() == "off":
                self.send_message_to_bot(chat_id, text="Log değiştirildi. OFF")
                self.settings['log'] = False
                json.dump(self.settings, open((os.getcwd()+'./data/settings.json'), 'w'))

            elif new_log_type.lower() != "on" or new_log_type.lower() != "off":
                self.send_message_to_bot(chat_id, text = "Girdi Yanlış Formatta Girildi!")

    def new_producs_alert(self, update, context):
        chat_id = update.effective_chat.id
        if chat_id in self.ADMIN_LIST:
            try : command, alert = (update.message.text).split(" ")
            except ValueError:
                self.send_message_to_bot(chat_id, "Girdi Yanlış Formatta Girildi!")
                return 0

            if alert.lower() == "on":
                self.send_message_to_bot(chat_id, "Yeni Ürünler uyarısı. ON")
                self.settings['new'] = True
                json.dump(self.settings, open((os.getcwd()+'./data/settings.json'), 'w'))
                
            elif alert.lower() == "off":
                self.send_message_to_bot(chat_id, "Yeni Ürünler uyarısı. OFF")
                self.settings['new'] = False
                json.dump(self.settings, open((os.getcwd()+'./data/settings.json'), 'w'))

    def API_info(self, update, context):
        chat_id = update.effective_chat.id
        if chat_id in self.ADMIN_LIST:
            api = update.message.text.lower()
            if api == "/api":
                requestLimit = int(ScraperAPIClient(self.API_KEY).account()['requestLimit'])
                requestCount = int(ScraperAPIClient(self.API_KEY).account()['requestCount'])
                msg = f'{ScraperAPIClient(self.API_KEY).account()}\nRemaining API requests {requestLimit - requestCount}'
                self.send_message_to_bot(chat_id, msg)
            else :
                self.send_message_to_bot(chat_id, "Girdi Yanlış Formatta Girildi!")
                return 0

    # Manage commands from Bot_1
    def bot_managment(self):
        while True:
            try:
                updater = Updater('1826283154:AAGeN6Jz00FaMtvTu_0I9ulP3t3KYdJIkaw', use_context=True)
                dp = updater.dispatcher    
                dp.add_handler(CommandHandler('help', self.help_))
                dp.add_handler(CommandHandler('discount', self.update_discount))
                dp.add_handler(CommandHandler('raise', self.update_raise))
                dp.add_handler(CommandHandler('new', self.new_producs_alert))
                dp.add_handler(CommandHandler('log', self.log_updater))
                dp.add_handler(CommandHandler('api', self.API_info))
                updater.start_polling()
                updater.idle()
            except Exception as e:
                print(f"exception {e}")

    def main_scraper(self):
        Thread(target=self.scraping_loop).start()
        self.bot_managment()


API_KEY = '<>'   # scraperAPI key
if __name__ == '__main__':
    scraper = AmazonScraper(API_KEY)
    scraper.main_scraper()
