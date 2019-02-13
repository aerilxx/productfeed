# productfeed
scrape all products from merchandise feed and website
source feed: rakuten, cj, shareasale, pepperjam

website: luxury brand who doesn't have cooporation relationship with marketers

includes category: product link, img, price, promotion, product info

upload to AWS Cloudsearch and build API Wrapper to connect image processing model results and user click

public API in structure:
  http://ec2-54-153-21-98.us-west-1.compute.amazonaws.com:5002/products/<brand_name>/<product_type>/<subid>
depper search: supply detail information of the products.
  http://ec2-54-153-21-98.us-west-1.compute.amazonaws.com:5002/products/mysearch?brand=<>&category=<>&color=<>&desc=<>&subid=123
  
  return most up-to-date products info
