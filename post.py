# POST Priority Order Staging Tool
import os
import subprocess
import sys
import time
import logging
import numpy as np
import pandas as pd
import glob
import platform
import argparse
import shutil
import tarfile
from dbfread import DBF
from datetime import datetime
from bdtlib import coreutils

# Set Up Arguments
parser = argparse.ArgumentParser()
parser.add_argument("date", help="Order date - MM_DD_YYYY")
parser.add_argument("email", help="Email where status update is sent. Multiple email addresses can be "
                                    "provided if they are separated by a comma."
                                    "user1@univ.edu,user2@univ.edu", type=str)
args = parser.parse_args()

inc_email_list = "bagl0025@umn.edu"     #Email list for incomplete orders over 2 months old
today = datetime.today().strftime("%m-%d-%Y-%H%M%S")
current_time = time.time()

# Directory locations
osinfo = platform.system()
if osinfo == "Windows":
    in_dir = "V:/pgc/data/staging/to_bw/"
    out_dir = "V:/pgc/data/staging/prio_ftp/"
else:
    in_dir = "/mnt/pgc/data/staging/to_bw/"
    out_dir = "/mnt/pgc/data/staging/prio_ftp/"

# Create Loggers
logger = logging.getLogger("logger")
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s %(levelname)s- %(message)s', '%m-%d-%Y %H:%M:%S')

# Stream logger
lso = logging.StreamHandler(sys.stdout)
lso.setLevel(logging.INFO)
lso.setFormatter(formatter)
logger.addHandler(lso)

log_dir = os.path.join(out_dir, "logs/")
log_file = os.path.join(log_dir, "Post_" + args.date + "_" + today + "_log.txt")
email = coreutils.Email(email_address=args.email,
                        job_type="Priority Orders complete status",
                        job_status="",
                        logfile=log_file,
                        region="",
                        res="",
                        keep_logfile=True)

# File logger
lfh = logging.FileHandler(log_file)
lfh.setLevel(logging.DEBUG)
lfh.setFormatter(formatter)
logger.addHandler(lfh)
logger.info("Checking order IDs against {} folder in to_bw".format(args.date))

# Check for .eot file before proceeding
if not os.path.isfile(os.path.join(in_dir, args.date + ".eot")):
    logger.warning("There is no .eot file for {}. Order check will have to be done manually.".format(args.date))
    email.update_status("No new data or data transfer not finished.")
    email.email_log()
    sys.exit(0)

# Make list of orders to process
work_list = glob.glob(os.path.join(out_dir, "*PGC_order*"))
for name in work_list:
    order_dir = os.path.join(name, "order/")
    imagery_dir = os.path.join(name, "imagery/")
    complete_file = os.path.join(name, "Order.complete")

    # Check for complete files and work on incomplete directories
    if os.path.isfile(complete_file):
        logger.info("{} -- Order Complete".format(name))
        file_time = os.stat(complete_file).st_atime
        file_age = current_time - file_time
        if (file_age / 86400) > 65:			# If complete file is older than ~ 2 months, delete image directory
            if os.path.exists(imagery_dir):
                logger.info("Complete file is over 2 months old {}. {} will be deleted".format(file_age / 86400, imagery_dir))
                shutil.rmtree(imagery_dir)

    else:
        logger.info("{} -- Order Incomplete".format(name))
        filelist = os.listdir(os.path.join(name, "order/"))

        if not os.path.exists(imagery_dir):
            os.mkdir(imagery_dir)

        df = pd.DataFrame()
        for file in filelist:
            if file.endswith(".xlsx"):
                orders = pd.read_excel(os.path.join(order_dir, file), header=None)
                df = df.append(orders)
                order_tmp = df.values.tolist()
                order_list = list(np.concatenate(order_tmp).flat)
                order_list = [x.strip(" ") for x in order_list]

        bw_data = []

        # Build list from directory in to_bw dir using provided date
        try:
            bw_data = os.listdir(os.path.join(in_dir, args.date))
        except:
            logger.info("Source directory {} not present: No new data".format(os.path.join(in_dir, args.date)))
            email.update_status("No new Data")
            email.email_log()
            sys.exit(0)

        # Loop over order_list from xcel, compare each id from order_list
        # to to_bw/<args.date> directory
        for catid in order_list:
            for bwid in bw_data:
                if catid == bwid[5:21]:

                    dst = os.path.join(imagery_dir, bwid)
                    src = os.path.join(in_dir, args.date, bwid)

                    # Make directory when match is found
                    if not os.path.exists(dst):
                        os.mkdir(dst)

                    # Hard link files in new directory
                    files_to_link = os.listdir(src)
                    for file in files_to_link:
                        if not os.path.exists(os.path.join(dst, file)):
                            try:
                                os.link(os.path.join(src, file), os.path.join(dst, file))
                            except Exception as e:
                                logger.error(coreutils.capture_error_trace())
                                logger.error("Unable to link files in image directory")
                                logger.error(e)
                                sys.exit(1)

        # Compare order_list to local imagery dir to check for completeness
        # If there is at least one image folder for each catalog ID
        # and the number of tar files match the product_shape.bdf record count it's complete
        imagery_local = []
        tmp_list = []

        imagery_local = [f for f in os.listdir(imagery_dir) if not f.startswith('.')]
        shape_file_check = 1    # assume shape file check is true unless an incomplete is found
        for cid in imagery_local:
            tmp_list.append(cid[5:21])
            cid_dir_list = [f for f in os.listdir(os.path.join(imagery_dir, cid)) if f.endswith(".tar")]
            target = os.path.join(cid, cid_dir_list[0])
            with tarfile.open(os.path.join(imagery_dir, target)) as tar:
                for tarinfo in tar:
                    if tarinfo.name.endswith("PRODUCT_SHAPE.dbf"):
                        tar.extract(tarinfo.name, name)
                        dbf_target = os.path.join(name, tarinfo.name)
            record_num = len(list(DBF(dbf_target)))
            tar_num = len(glob.glob1(os.path.join(imagery_dir, cid), "*.tar"))
            if tar_num != record_num:
                shape_file_check = 0
                logger.debug("{} -- {} -- record count {}, tar file count {} ".format(name, target, record_num, tar_num))
            shutil.rmtree(os.path.join(name, "imagery_ingest"))    # Cleanup extracted tar folder

        imagery_local_set = set(tmp_list)
        order_list_set = set(order_list)

        # Check for order completeness
        if imagery_local_set == order_list_set and shape_file_check == 1:
            try:
                with open(complete_file, "w") as fp:
                    pass
                logger.info("{} -- This order was completed on this run".format(name))
            except Exception as e:
                logger.error(coreutils.capture_error_trace())
                logger.error("Unable to write complete file")
                logger.error(e)
                sys.exit(1)
        if (imagery_local_set != order_list_set):
            logger.debug("Catalog IDs missing from imagery directory {}".format(order_list_set - imagery_local_set))
            dir_age = (current_time - os.stat(name).st_atime) / 86400
            if dir_age > 65:
                try:
                    with open(complete_file, "w") as fp:
                        pass
                    logger.info("{} -- This order was marked completed on this run because it is "
                                "older than 2 months but still incomplete".format(name))
                except Exception as e:
                    logger.error(coreutils.capture_error_trace())
                    logger.error("Unable to write complete file")
                    logger.error(e)
                    sys.exit(1)
                subprocess.Popen("echo '{} was marked complete due to being 2 months old. It is not complete, the "
                             "MFP and the DigItal Globe (https://discover.digitalglobe.com/) site should be checked "
                             "for catalog IDs. The logfile will contain more information.' "
                             "| mail -s 'Incomplete Job Update' {}".format(name, inc_email_list), shell=True)

logger.removeHandler(lfh)
logger.removeHandler(lso)

email.update_status(args.date)
email.email_log()
