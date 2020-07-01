# gene-variant-extractor

*This was my first experience coding in Python.*

**Date:** Summer 2018

This repository contains **python** code used to parse and organize gene variants from supplemental data files associated with human genomics research articles.

Code is in **src**.

Example output with some files (not all that are usually outputted) containing extracted data and logs of files processed is in **output.zip**.

## Installation

Install the following packages on your computer if you don't already have them:
- pandas
- xlrd
- docx
- docx2csv
- XlsxWriter
- antiword
- pdftotext --> command line tool, although there may be [alternative python library](https://pypi.org/project/pdftotext/)

```
pip install pandas
pip install xlrd
pip install docx
pip install docx2csv
pip install XlsxWriter
sudo apt-get install antiword unrtf poppler-utils libjpeg-dev
```
pdftotext: http://macappstore.org/pdftotext/ or `pip install pdftotext` (python package)

## Getting Started

1. Place code and `genelist.txt` in directory that contains folder of supplemental data files.
2. Run `suppdata_scraper.py`. You will be prompted in the terminal to input the name of folder containing files and your name.
3. As script runs, the following should happen:
* File progress will be logged via different `.txt` files. Check `files_processed.txt` for overall progress.
* Terminal will print statements indicating filename with index currently being processed. Files are not processed in exact index order because of multiprocessing.
* `.txt` files will be created in `dataframes` folder for every individual file that contains data.
* Files that may contain amino acids or nucleotides are copied to `manual` folder.
* `.txt` and `.xlsx` files will be created in the `workspace` folder for parsing purposes.
4. Once `suppdata_scraper.py` is done running, run `dataframe.py`. When prompted, you should input the name of the folder containing the dataframes. Once this is done running, you should have `masterlist.txt` contained in the `output` folder.

## About Code

* `suppdata_scraper.py` is the main scraper program used to parse files and extract gene variants. 

* `dataframe.py` is for combining all the dataframes containing extracted data from different files into a single masterlist file with all the extracted data. *This should be used in instances where suppdata_scraper.py hits a roadblock and is not able to concatenate all the dataframes during its run.*

* `big_manual.py` is for screening and prioritizing large files that contain amino acids and/or nucleotides and need to be manually extracted.

* `manual.py` is for screening files containing amino acids and/or nucleotides and counting the number of occurrences of amino acids and/or nucleotides. These files will need to be manually extracted.

### How suppdata_scraper.py Works

**Input:** Directory of supplemental data files scraped from the web. These files can be any of the following type:

- pdf
- doc/docx
- txt
- xls/xlsx
- csv/tsv

**Output:** 

* `output` folder with following:
	* `masterlist.txt` --> **Main output.** All gene variants are stored with files they came from. Also `masterlist.csv` and `masterlist.xlsx`, which contain same info in different file type.
	* Following `.txt` files that characterize data:
		- `files_processed.txt`: filenames and index in list
		- `bad_files.txt`: files that produce an error
		- `good_files.txt`: files that contain gene variants
		- `manual.txt`: files that contain nucleotides or amino acids
		- `files_ignored.txt`: Other file types such as media files that are not relevant
		- `variant_counts.txt`: Counts for total number of different gene variants for each file that contains data
		- `process_time.txt`: File size and time it takes for script to process each file
* `dataframes` folder with dataframe files containing data extracted from all files
* `manual` folder with files that need to be manually extracted and have been copied over
* `big_files_manual` folder containing large files that need to be manually extracted and have been copied over

**Some more details:**
- Scraper finds genes by comparing against genelist.txt and finds different variants using regular expressions.
- For pdf, doc, and txt files, the scraper goes through line by line and pulls out gene and variant matches.
- For xlsx and xls files, the scraper goes through every cell row by row and pulls out gene and variant matches.
- For docx files, the scraper extracts any tables that it finds, converts them into xlsx files, and then follows the same procedure for a xlsx file. 

This methodology, while perhaps not the most efficient, proved to be pretty accurate and ensured that associations between genes and variants on the same lines/rows in files were in most cases maintained during the data extraction.
