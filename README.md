# ece143-final-proj-wine
ECE143 final project for wine reviews.

# To install required packages:
`pip install -r requirements.txt`

## Layout of Repo:
At the top level, we have overall files.
 - init.py: used to initialize the database
 - requirements.txt: the requirements needed for our repo
 - RUNME.ipynb: the notebook file to create the visuals used for our presentation
 - ECE 143 Final Presentation Deck.pptx.pdf: a pdf version of our final presentation.
Top level directories:
 - data: contains the .csv files we used as our data:
   - winemag-data-130k-v2.csv: the original raw data
   - winemag-data-130k-v2_cleaned.csv: the original raw data but with new columns added for cleaning variety, winery, and province columns, adding these as new columns suffixed by "cleaned"
   - winemag-data-130k-v2_new.csv: same as winemag-data-130k-v2_cleaned.csv, but replaces the columns those suffixed by "cleaned" are for by those in winemag-data-130k-v2_cleaned.csv that are suffixed by "cleaned"; ie, presents that cleaned data but only with the original column names.
   - adjectives_nouns.csv: contains adjectives and nouns from the description column of our data.
 - data_cleaning: contains code to clean and otherwise process and filter the data:
   - data_cleaning.py: contains basic data cleaning functions (such as dropping null entries)
   - text_filter.py: is used to process text, count its word frequency, and plot word clouds of it
 - database: Contains the database (file) and related functionality:
   - db_constants.py: constants for the database
   - db_op.py: contains basic operations for working with the database
   - wine_init.db: the database
 - ipynb_archive: rather than deleting our old ipynb files (that we collected into RUNME.ipynb), we instead created this directory to store them.
 - point_prediction: Contains files for using a neural network to predict the point rating of a wine based on its textual description.
 - visuals: a directory that contains miscellaneous visuals
   - wine-glass-outline-hi.png: is used as the background shape for our word clouds
- wine_stat: a directory with basic statistics functionality
  - common_stat.py: used for common statistics operations, as well as general manipulations of database tables as a result of them
  - freq.py: used to come up with the top n frequencies grouped by various columns. Remaining entries for a given table are grouped into a row named "Other" for that respective table.
  - null_info.py: a file for specifically handling null info, such as printing out a null info summary for a table.
  - vis.py: used for visualizing data, such as for custom plotting functionality. 