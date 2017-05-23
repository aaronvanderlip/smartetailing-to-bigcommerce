# Imports QBP inventory to BigCommerce
import datetime
from email.mime.text import MIMEText
from lxml import objectify
import ftplib
import ftputil
import os
import pickle
import sets
import smtplib
import subprocess
import time
import urllib
import zipfile
from bigcommerce import api


API_HOST = 'https://store-8e41b.mybigcommerce.com'
API_KEY = 'API_KEY'
API_USER = 'API_USER'
API_PATH = '/api/v2'

URL_KEY = 'URL_KEY'
QPB_MERCHANT_ID = '00000'
QBP_MERCHANT_URL = 'http://qbp.com/webservices/xml/QBPSync.cfc?'

FULL_CATALOG = "%smethod=FullCatalog&merchant=%s&URLKey=%s" % (
    QBP_MERCHANT_URL, QPB_MERCHANT_ID, URL_KEY)
DISCONTINUED = "%smethod=DiscontinuedItems&NumberOfDays=180&merchant=%s&URLKey=%s" % (
    QBP_MERCHANT_URL, QPB_MERCHANT_ID, URL_KEY)
SE_HOURLY = "%smethod=HourlyUpdates&merchant=%s&URLKey=%s" % (
    QBP_MERCHANT_URL, QPB_MERCHANT_ID, URL_KEY)
SE_DAILY = "%smethod=DailyUpdates&merchant=%sNumberOfDays=1%s" % (
    QBP_MERCHANT_URL, QPB_MERCHANT_ID, URL_KEY)
IMAGE_UPDATES = "%s?method=ImageUpdates&merchant=%s0&URLKey=%s" % (
    QBP_MERCHANT_URL, QPB_MERCHANT_ID, URL_KEY)

FTP_HOST = 'FTP_HOST'
FTP_USER = 'FTP_USER'
FTP_PASSWORD = 'FTP_PASSWORD'

BC_FTP_HOST = 'server1300.bigcommerce.com'
BC_FTP_USER = 'BC_FTP_USER'
BC_FTP_PASSWORD = 'BC_FTP_PASSWORD'

# Define local storage paths for zip archives.
STORAGE = '/home/aaronv/projects/urbane/storage/'
ZIPFILES = '/home/aaronv/projects/urbane/storage/zipfiles/'
IMAGES = '/home/aaronv/projects/urbane/storage/images/'

conn = api.Connection(API_HOST, API_PATH,  API_USER, API_KEY)

EMAIL_USER = 'EMAIL_USER'
EMAIL_PASSWORD = 'EMAIL_PASSWORD'
EMAIL = 'youremail@example.com'


def email_updates(subject=None, to_addrs=None, message_text=None):
    username = EMAIL_USER
    password = EMAIL_PASSWORD
    msg = MIMEText(message_text)
    from_addr = EMAIL

    msg['Subject'] = subject
    msg['From'] = from_addr
    msg['To'] = " ,".join(to_addrs)

    server = smtplib.SMTP('smtp.gmail.com:587')
    server.starttls()
    server.login(username, password)
    server.sendmail(from_addr, to_addrs, msg.as_string())
    server.quit()


def update_inventory(period='daily'):

    print "downloading file"

    updates = []
    if period == 'daily':
        se_updates = parse_qbp(fetch_daily_updates('StockUpdates.xml'))
    elif period == 'hourly':
        se_updates = parse_qbp(fetch_hourly_updates())
    else:
        return "Specify either 'daily' or 'hourly'"

    print "begin lookups "
    store_products = api.Products(client=conn)
    # All current products from the store
    all_current_products = store_products.get_all()
    # Create a mapping so existing products can be looked up by key
    existing_products = {}
    for prod in all_current_products:
        existing_products.setdefault(prod.sku, prod)

    for prod in se_updates:
        sku = prod.get('sku')
        store_prod = existing_products.get(sku, None)
        if store_prod is not None:
            se_inv = prod.get('quantity')
            store_inv = store_prod.inventory_level
            if (int(se_inv) != int(store_inv)):
                text = "SKU %s %s Old store inventory level: %s. Updated store inventory level: %s " % (
                    sku, store_prod.name, store_prod.inventory_level, prod.get('quantity'))
                store_prod.inventory_level = se_inv
                # FIXME we have to re assign the connection
                store_prod.client = conn
                store_prod.update_field('inventory_level', se_inv)
                updates.append(text)
                print text

            if conn.remaining_requests < 100:
                print "sleeping"
                time.sleep(60)

    if len(updates) > 0:
        date = datetime.datetime.today()
        message_text = "%s items were updated \n\n %s" % (
            len(updates), "\n\n".join(updates))
        subject = "%s storeinventory updates for %s %s " % (
            len(updates), date.isoformat(), date.time().isoformat())
        email_updates(
            to_addrs=[EMAIL],
            subject=subject,
            message_text=message_text)
    return


def remove_discontinued_products():
    store_products = api.Products(client=conn)
    discontinued = parse_qbp(fetch_discontinued())
    updates = []
    for prod in discontinued:
        sku = prod.get('sku')
        store_prod = store_products.get_by_sku(sku)
        if store_prod:
            store_prod.delete()
            text = "SKU %s %s " % (sku, store_prod.name)
            print text
            updates.append(text)

    message_text = "%s discontinued items were deleted \n\n %s" % (len(updates), "\n\n".join(updates))
    subject = "%s removal of discontinued items" % datetime.date.today().isoformat()
    email_updates(to_addrs=[EMAIL], subject=subject, message_text=message_text)


def create_full_catalog_index():
    """ Creates a flat file with a list of SKUS """
    full_catalog = parse_qbp(fetch_full_catalog())
    # save pickle of all catalog skus
    existing_skus = open(STORAGE+'existing_skus', 'w+')
    full_catalog = sets.Set([prod.get('sku') for prod in full_catalog])
    pickle.dump(full_catalog, existing_skus)


def add_new_from_full_catalog():
    """ Creates a list of new products by comparing the entire catalog with
    the existing SKUS in the online store """
    # NOTE: the list of skus must be exported from BigCommerce before this is
    # run

    full_catalog = parse_qbp(fetch_full_catalog())
    bc_ftp = ftputil.FTPHost(
        BC_FTP_HOST, BC_FTP_USER, BC_FTP_PASSWORD,
        session_factory=ftplib.FTP_TLS)

    bc_ftp.chdir('/exports')
    sku_export_file_path = bc_ftp.listdir('/exports').pop()
    print "Downloading %s" % sku_export_file_path
    sku_export_file = bc_ftp.download(
        sku_export_file_path, STORAGE + 'sku_export')
    sku_export_file = open(STORAGE + 'sku_export', 'r')
    current_skus = parse_qbp(sku_export_file)

    # create a list of skus that are currently in the store
    current_skus = [el.Product_SKU for el in current_skus]

    # create a dict for looking up new products
    new_prods = {}
    for prod in full_catalog:
        new_prods.setdefault(prod.get('sku'), prod)

    # if we find a matching sku, we delete it
    # we only want sku from SE that are not already
    # in BC
    for sku in current_skus:
        prod = new_prods.get(sku, None)
        if prod:
            del(new_prods[sku])

    add_new_products(new_prods.values())


def add_new_products(se_updates=None):
    """ Find products in SE inventory that are not in the current
    store by comparing SKU and add them to the NEW-1 category"""
    if se_updates is None:
        se_updates = parse_qbp(fetch_daily_updates('DailyUpdates.xml'))

    # Load the existing_skus. We don't want to add any SKUS that were added during
    # a previous operation. This covers the case were SKUS were added to the store then
    # later deleted.
    existing_skus = pickle.load(open(STORAGE+'existing_skus'))

    # images have to be uploaded to BigCommerce first
    images = fetch_product_images()

    store_products = api.Products(client=conn)
    store_brands = api.Brands(client=conn)
    store_images = api.Image(client=conn)
    new_products = []

    # se_updates should not including any SKUS that are in existing_skus
    # compare se_updates to existing_skus
    for prod in se_updates:
        sku = prod.get('sku')
        # Remove the incoming product if it is in existing
        # We do an additional check for price updates as this product update is
        # the only source of this info
        if sku in existing_skus:
            # set price
            if conn.remaining_requests < 100:
                print "sleeping"
                time.sleep(60)
            store_prod = store_products.get_by_sku(sku)
            if store_prod:
                price = prod.get('palPrice')
                if price == '0.00':
                    price = prod.get('myPrice')
                # msrplow must override any other pricing info
                msrpLow = prod.get('msrpLow', '0.00')
                if msrpLow != '0.00':
                    price = msrpLow
                if (float(price) != float(store_prod.price)):
                    store_prod.client = conn
                    store_prod.update_field('price', price)
                    store_prod.update_field('cost_price', prod.get('baseCost'))
                    store_prod.update_field('retail_price', prod.get('msrp'))
                    text = "SKU %s %s Old store price: %s. Updated store price: %s " % (
                        sku, store_prod.name, float(store_prod.price), price)
                    new_products.append(text)
                    print text
            se_updates.pop(se_updates.index(prod))

    for prod in se_updates:
        # Any number of the network options can throw an exception
        try:
            sku = prod.get('sku')
            # Try to fetch the product.
            store_prod = store_products.get_by_sku(sku)
            # remaing requests set after first request
            if conn.remaining_requests < 100:
                print "sleeping"
                time.sleep(60)
            # If there is no existing SKU we add it
            if not store_prod:
                # Defensively set price
                price = prod.get('palPrice')
                if price == '0.00':
                    price = prod.get('myPrice')

                # msrplow must override any other pricing info
                msrpLow = prod.get('msrpLow', '0.00')
                if msrpLow != '0.00':
                    price = msrpLow
                brand_name = prod.get('brandName', None)
                if brand_name:
                    try:
                        brand_id = store_brands.get_by_name(brand_name)
                    except:
                        brand_id = None
                page_title = "%s at The Urbane Cyclist" % prod.get('name')

                fields = {
                    'name': prod.get('name'),
                    'sale_price': price,
                    'price': price,
                    'cost_price': prod.get('baseCost'),
                    'retail_price': prod.get('msrp'),
                    'upc': prod.get('UPC'),
                    'page_title': page_title,
                    'availability_description': "Usually ships in 2-3 days",
                    'categories': [1561],
                    'type': 'physical',
                    'availability': 'available',
                    'sku': prod.get('sku'),
                    'inventory_level': prod.get(
                        'quantity',
                        0),
                    'inventory_tracking': 'simple',
                    'weight': prod.freightdata.get(
                        'weight',
                        0),
                    'width': prod.freightdata.get(
                        'width',
                        0),
                    'depth': prod.freightdata.get(
                        'length',
                        0),
                    'height': prod.freightdata.get(
                        'height',
                        0),
                    'description': unicode(
                        prod.description)}

                if brand_id:
                    fields.setdefault('brand_id', brand_id)
                try:
                    new_prod = store_products.add(fields)
                except:
                    name = "UPDATED -- %s" % prod.get('name')
                    fields.update({'name': name})
                    print name
                    try:
                        new_prod = store_products.add(fields)
                    except:
                        pass

                prod_images = prod.get('largeImage', None)
                if prod_images:
                    prod_images = prod_images.split(',')

                # should be its own function
                for prod_image in prod_images:
                    image_path = images.get(prod_image, None)
                    if image_path:
                        # SFTP connection for talking to BigCommerce
                        bc_ftp = ftplib.FTP_TLS(
                            BC_FTP_HOST, BC_FTP_USER, BC_FTP_PASSWORD)
                        bc_ftp.cwd('/product_images/import')
                        f = open(image_path, 'rb')
                        bc_ftp.storbinary('STOR %s' % prod_image, f)
                        f.close()
                        image_fields = {'image_file': prod_image}
                        store_images.create(new_prod.get('id'), image_fields)
                        # Delete image from Big Commerce FTP server to save
                        # space
                        bc_ftp.delete(prod_image)
                # get the newly added product
                text = prod.get('name') + " " + prod.get('sku')
                print text
                new_products.append(text)

                # add sku to existing_sku
                # this is the sku loggin feature to ensure
                # they are not added again
                existing_skus.add(sku)
                # save pickle
                existing_skus_file = open(STORAGE+'existing_skus', 'w+')
                pickle.dump(existing_skus, existing_skus_file)

                if conn.remaining_requests < 100:
                    print "sleeping"
                    time.sleep(60)
        except:
            print "punting %s " % prod.get('sku')
    # only send emails if there are updates
    if len(new_products) > 0:
        date = datetime.datetime.today()
        message_text = "%s new items were added to the store \n\n %s" % (
            len(new_products), "\n\n".join(new_products))
        subject = "%s new items added to store on %s " % (
            len(new_products), date.isoformat())
        email_updates(
            to_addrs=[EMAIL],
            subject=subject,
            message_text=message_text)


def fetch_discontinued():
    try:
        temp_file, hdrs = urllib.urlretrieve(DISCONTINUED)
    except IOError:
        print "can't get file"
        return
    f = open(temp_file)
    return f


def fetch_full_catalog():
    print "downloading full catalog"
    try:
        temp_zip, hdrs = urllib.urlretrieve(FULL_CATALOG)
    except IOError:
        print "can't get file"
        return
    try:
        z = zipfile.ZipFile(temp_zip)
    except zipfile.error:
        print "bad zip"
        return

    return z.open('QBPSync.xml')


def fetch_product_images():
    """ Fetches all images from SmartEtailing"""

    UPDATES_PATH = 'qbp cycling catalog/updates/'
    FULL_PATH = '/qbp cycling catalog/full/'
    product_images = {}

    # Setup FTP connection
    host = ftputil.FTPHost(FTP_HOST, FTP_USER, FTP_PASSWORD)

    # get a list of all the update files
    # walk returns root, dirs, files
    # so we grab the last item
    imagezips = host.walk(UPDATES_PATH).next()[2]

    # create full paths for downloading
    imagezips = [(UPDATES_PATH + zip_file, zip_file) for zip_file in imagezips]

    # Treat the full archive diffently and prepend to the imagefiles list
    full_name = host.listdir(FULL_PATH)[0]
    full_path = FULL_PATH + full_name
    full_archive = (full_path, full_name)
    imagezips.append(full_archive)

    for file in imagezips[-4:]:
        print "Downloading %s" % file[0]
        download_and_unzip(file[0], file[1])

    # We only care for large images for now
    # as the SE archives seem strangely structured
    walker = os.walk(IMAGES + 'large')
    root, dirs, files = walker.next()

    for file in files:
        path = IMAGES + 'large/' + file
        product_images.setdefault(file, path)

    # We return a mapping of image names and paths to files on disc
    return product_images


def download_and_unzip(source_path, filename):

    host = ftputil.FTPHost(FTP_HOST, FTP_USER, FTP_PASSWORD)
    # download
    dest = ZIPFILES + filename
    file = host.download_if_newer(source_path, dest, 'b')

    # unpack
    if file:
        subprocess.call(['unzip', '-u', '-o', dest, '-d', IMAGES])


def fetch_hourly_updates():
    file, hders = urllib.urlretrieve(SE_HOURLY)
    return file


def fetch_daily_updates(file):
    """ Return an XML representation of the hourly feed """
    try:
        temp_zip, hdrs = urllib.urlretrieve(SE_DAILY)
    except IOError:
        print "can't get file"
        return
    try:
        z = zipfile.ZipFile(temp_zip)
    except zipfile.error:
        print "bad zip"
        return

    return z.open(file)


def parse_qbp(file):
    """ Returns product elements from parsed file"""
    tree = objectify.parse(file)

    # get root
    root = tree.getroot()

    # get products
    prods = root.getchildren()

    return prods
