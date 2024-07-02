# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
# import psycopg2
from itemadapter import ItemAdapter


class BookscraperPipeline:
    def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        # remove whitespaces from strings
        field_names = adapter.field_names()
        for field_name in field_names:
            if field_name != 'description':
                value = adapter.get(field_name)
                adapter[field_name] = value[0].strip()
        
        # switch category and product type to lowercase
        lowercase_keys = ['category', 'product_type']
        for lowercase_key in lowercase_keys:
            value = adapter.get(lowercase_key)
            adapter[lowercase_key] = value.lower()

        # convert price to float
        price_keys = ['price', 'price_excl_tax', 'price_incl_tax', 'tax']
        for price_key in price_keys:
            value = adapter.get(price_key)
            value = value.replace('£', '')
            adapter[price_key] = float(value)

        # extract number of books from availability that are in stock
        availability_string = adapter.get('availability')
        split_string_array = availability_string.split('(')
        if len(split_string_array) < 2:
            adapter['availability'] = 0
        else:
            availability_array = split_string_array[1].split(' ')
            adapter['availability'] = int(availability_array[0])

        # convert number of reviews to integer
        number_of_reviews = adapter.get('number_of_reviews')
        adapter['number_of_reviews'] = int(number_of_reviews)

        # convert star rating to integer
        star_rating = adapter.get('star_rating')
        star_rating_text = star_rating.lower()
        if star_rating_text == 'zero':
            adapter['star_rating'] = 0
        elif star_rating_text == 'one':
            adapter['star_rating'] = 1
        elif star_rating_text == 'two':
            adapter['star_rating'] = 2
        elif star_rating_text == 'three':
            adapter['star_rating'] = 3
        elif star_rating_text == 'four':
            adapter['star_rating'] = 4
        elif star_rating_text == 'five':
            adapter['star_rating'] = 5

        return item

# class SaveToPostgreSQLPipeline:
#     def __init__(self):
#         self.conn = psycopg2.connect(
#             host='localhost',
#             user='postgres',
#             dbname='books',
#         )
#         self.cursor = self.conn.cursor()
#         self.cursor.execute("""
#             CREATE TABLE IF NOT EXISTS books (
#                 id SERIAL PRIMARY KEY,
#                 url TEXT,
#                 title TEXT,
#                 upc VARCHAR(255),
#                 product_type VARCHAR(255),
#                 price_excl_tax FLOAT,
#                 price_incl_tax FLOAT,
#                 tax FLOAT,
#                 availability INT,
#                 number_of_reviews INT,
#                 star_rating INT,
#                 category VARCHAR(255),
#                 description TEXT,
#                 price FLOAT
#             )
#         """)

#     def process_item(self, item, spider):
#         self.cursor.execute(""" INSERT INTO books (
#             url,
#             title,
#             upc,
#             product_type,
#             price_excl_tax,
#             price_incl_tax,
#             tax,
#             availability,
#             number_of_reviews,
#             star_rating,
#             category,
#             description,
#             price
#         ) values (
#             %s,
#             %s,
#             %s,
#             %s,
#             %s,
#             %s,
#             %s,
#             %s,
#             %s,
#             %s,
#             %s,
#             %s,
#             %s
#         )""", (
#             item['url'],
#             item['title'],
#             item['upc'],
#             item['product_type'],
#             item['price_excl_tax'],
#             item['price_incl_tax'],
#             item['tax'],
#             item['availability'],
#             item['number_of_reviews'],
#             item['star_rating'],
#             item['category'],
#             item['description'],
#             item['price']
#         ))
#         self.conn.commit()
#         return item
    
#     def close_spider(self, spider):
#         self.cursor.close()
#         self.conn.close()