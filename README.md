# productfeed
scrape all products from merchandise feed and website
source feed: rakuten, cj, shareasale, pepperjam

website: luxury brand who doesn't have cooporation relationship with marketers

includes category: product link, img, price, promotion, product info

upload to AWS Cloudsearch and build API Wrapper to connect image processing model results and user click

public API in structure:
  http://'secret'/<brand_name>/<product_type>/<subid>
depper search: supply detail information of the products.
  http://'secret'/products/mysearch?brand=<>&category=<>&color=<>&desc=<>&subid=123
  
  return most up-to-date products info

for update function, run celery parallel uploading & uploading:
download : celery -A shareAsale worker --loglevel=debug
           celery -A cj_celery worker --loglevel=debug
           celery -A rakuten_celery worker --loglevel=debug
          
run main function to call celery workers

upload : celery -A upload_task worker --loglevel=debug
run main: python upload_start.py

worker monitor: celery -A upload_task flower --port=5555
check work failure:      http://localhost:5555/broker
