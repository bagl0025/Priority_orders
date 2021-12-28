### [post.py](post.py)
Priority Order Staging Tool. User Services staff puts xlsx files in a directory per priority order in 
/mnt/pgc/data/staging/prio_ftp/<ordername>/order/ where ordername is like "PGC_order_2021apr13_UserService_Priority".
post.py scrapes the xlsx files for catalog IDs unless there is an Order.complete file in the order subdir. 
Checks the daily dump folder /mnt/pgc/data/tmp-stg/staging/to_bw/<date_dir> where date_dir is like "04_15_2021" 
from Digital Globe's ftp stream for folders containing those catalog IDs. Logs are dumped in the /mnt/pgc/data/staging/
prio_ftp/logs/ directory.

If anything is found, the images are hard-linked into a subdirectory in  /mnt/pgc/data/staging/prio_ftp/<ordername>/imagery/ 
by strip ID.
If ever an order is completely fulfilled an Order.complete file is put in the order dir and that order is not checked 
in subsequent runs. “Complete” is defined as at least one scene from every catalogID is present in the imagery dir 
AND number of scenes per strip ID directory equals the number of rows in the *PRODUCT_SHAPE.shp file in the scene tar/GIS_FILES dir.
2 months after the Order.complete file is created, the imagery will be deleted as it will now exist on tape and in the mfp.

If an order remains incomplete for 2 months it will be marked as complete and an email is sent. The recipient
will check the MFP and Digital Globe to find out why it is still incomplete and the catalog IDs can be
reordered if necessary. After 2 months the order will be cleaned up like a normal complete order.

Default destination directory (on PGC): `/mnt/pgc/data/staging/prio_ftp/`  
Default source directory (on BW): `/mnt/pgc/data/staging/to_bw/`

### Logs
A log folder is located in the default destination folder and contains at least one log file per run. 
A shortened version of the logfile will be emailed to everyone on the email list each run. Additional
information about order completeness can be found in the logfile.

### Example usage
This job is run automatically by cron_globus_source_imagery_to_bw.sh each night.
The cron job contains a list of email recipients to be notified.

Manual Usage
```
python post.py mm_dd_yyyy user1@univ.edu,user2@univ.edu
note: You must use quotes if there are spaces after the comma(s)
``` 

Will return nonzero result on error.
