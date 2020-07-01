##For scanning big files

"""
User inputs the folder containing the files for processing
Example
folder = '/test/'
"""
##Get modules needed

import os
import shutil
import re
import pandas as pd
import xlrd
from xlrd import open_workbook
import csv
import time
import zipfile    
from zipfile import BadZipfile
from zipfile import ZipFile
import docx
import docx2csv
import concurrent.futures
from docx2csv import extract_tables,extract

#For logging info about file processing. Gets processing time and stores this information in txt file. Also keeps record of every file completed.
#Master function that is executed against the entire file list
def masterfunction(file):
	try:
	#start time
		time_initial = time.time()

		def process_info():
			time_final = time.time()
			extraction_time = time_final - time_initial
			data_extract_str = file + '\t' + str(file_size) + '\t' + str(extraction_time) + '\n'
			print(data_extract_str)
			fp.write(file_str)
			pt.write(data_extract_str)
			return

		def findkeywords(filepath):

			try: 
				input_file = open(filepath,'r')
				lines = input_file.readlines()
				input_file.close()

			except:
				print('%s cannot be opened' % (file))
				b.write(file)
				b.write('\n')
				return

			#For all matches in each line, store them in a list that will be appended to a larger list. 
			#This is to things in tact with the line they came from.
			for line in lines:
				for item in line.split():
					rsid_match = re.search(regex_rsid,item)
					c_match = re.search(regex_c, item)
					p_match = re.search(regex_p,item)
					v_match = re.search(regex_v,item)
					cdna_match = re.search(regex_cdna,item)
					# nucleotide_match = item in nucleotides
					# aa_1_match = item in amino_acids_1_letter
					# aa_3_match = item in amino_acids_3_letter
		
					if (rsid_match or c_match or p_match or v_match or cdna_match):
						print('%s is high priority!' % file)
						h.write(file)
						h.write('\n')
						shutil.move(file_path, high_priority)
						return

		#Behaves nearly identical to findkeywords, except that function iterates row by row and column by column, scanning every cell
		def findkeywords_excel(filepath):
			try:
				book = open_workbook(filepath)
			except:
				print('%s cannot be opened' % (file))
				b.write(file)
				b.write('\n')
				return

			for number in range(book.nsheets):
				sheet = book.sheet_by_index(number)
				for i in range(sheet.nrows):
					row = sheet.row_values(i)
					for cell in row:
						cell_str = str(cell)
						rsid_match = re.search(regex_rsid,cell_str)
						c_match = re.search(regex_c,cell_str)
						p_match = re.search(regex_p,cell_str)
						v_match = re.search(regex_v,cell_str)
						cdna_match = re.search(regex_cdna,cell_str)
						# nucleotide_match = cell_str in nucleotides
						# aa_1_match = cell_str in amino_acids_1_letter
						# aa_3_match = cell_str in amino_acids_3_letter

						if (rsid_match or c_match or p_match or v_match or cdna_match):
							print('%s is high priority!' % file)
							h.write(file)
							h.write('\n')
							shutil.move(file_path, high_priority)
							return
		
		#Behaves exactly the same as findkeywords_excel. A few different functions/variables are used because of different module.
		def findkeywords_csv(filepath):
			
			if file_extension == '.tsv':
				try:
					tsvin = open(filepath,newline = '')
					reader = csv.reader(tsvin,delimiter = '\t')
				except:
					print('%s cannot be opened' % (file))
					b.write(file)
					b.write('\n')
					return
			else:
				try:
					csvin = open(filepath,newline = '')
					reader = csv.reader(csvin)
				except:
					print('%s cannot be opened' % (file))
					b.write(file)
					b.write('\n')
					return
			try:
				for row in reader:
					for cell in row:
						rsid_match = re.search(regex_rsid,cell)
						c_match = re.search(regex_c,cell)
						p_match = re.search(regex_p,cell)
						v_match = re.search(regex_v,cell)
						cdna_match = re.search(regex_cdna,cell)
						# nucleotide_match = cell in nucleotides
						# aa_1_match = cell in amino_acids_1_letter
						# aa_3_match = cell in amino_acids_3_letter

						if (rsid_match or c_match or p_match or v_match or cdna_match):
							print('%s is high priority!' % file)
							h.write(file)
							h.write('\n')
							shutil.move(file_path, high_priority)
							if file_extension == '.csv':
								csvin.close()
							elif file_extension == 'tsv':
								tsvin.close()
							return

			except (UnicodeError,UnicodeDecodeError,KeyError,IndexError):
				b.write(file)
				b.write('\n')
				print('Something is really messed up with this file')
				return

		##Main body of masterfunction, in which it calls other functions. This is essentially in a loop and used on every file.
		
		#Text Files are created for storing information about the files that are processed. These are all located in the "output" folder
		with open(output + 'files_processed.txt','a') as fp, open(output + 'high_priority.txt','a') as h, open(output + 'files_ignored.txt','a') as ig, open(output + 'process_time.txt','a') as pt, open(output + 'bad_files.txt','a') as b:
			file_index = file_list.index(file)
			file_str = file + '\t' + "%s/%s" % (file_index,file_list_size) + '\n'
			print("File Index: %s/%s" % (file_index,file_list_size))
			print('File: %s' % (file))
			file_path = directory + folder + file
			file_size = os.path.getsize(file_path)
			file_extension = os.path.splitext(file)[1]

			##Copy all files greater than 20 MB out of the file folder and into another folder called big_files_manual for manual processing
			##These files tend to take a long time to process, so they can be processed faster via manual review.

				#If excel file, use findkeywords_excel, which uses xlrd module.
			if file_extension == '.xlsx' or file_extension == '.xls':
				getexcel = findkeywords_excel(file_path)
				info = process_info()
				return 

			##If csv or tsv file, use findkeywords_csv, which uses csv module.
			elif file_extension == '.csv' or file_extension == '.tsv':
				getcsv = findkeywords_csv(file_path)
				info = process_info()
				return 

			#If pdf, convert to txt file using command line function "pdftotext", and use findkeywords
			elif file_extension == '.pdf':
				newfilepath = workspace + file.replace('.pdf','.txt')
				os.system("pdftotext -layout '%s' '%s'" % (file_path, newfilepath))
				getwords_pdf = findkeywords(newfilepath)
				info = process_info()
				return 

			#If file_extension contains '&type=printable', this is a pdf. Convert to txt file using command line function "pdftotext", and use findkeywords
			elif '&type=printable' in file_extension:
				newfilepath = workspace + re.sub(r'\..*','.txt',file)
				os.system("pdftotext -layout '%s' '%s'" % (file_path, newfilepath))
				getwords_pdf = findkeywords(newfilepath)
				info = process_info()
				return 

			#If docx, extract any tables from docx files and convert into xlsx files using module docx2csv, then use findkeywords_excel.
			elif file_extension == '.docx':
				try:
					##Copy the original docx file over to workspace, extract there 
					shutil.copy(file_path,workspace)
					newfilepath = workspace + file
					tables = extract_tables(newfilepath)
					extract(filename = newfilepath,format='xlsx',singlefile=True)
					workspace_file = newfilepath.replace('.docx','.xlsx')
					getdocx_tables = findkeywords_excel(workspace_file)
					info = process_info()
					return getdocx_tables
				except (BadZipfile,KeyError,IndexError,ValueError,UnicodeError,UnicodeDecodeError,docx.opc.exceptions.PackageNotFoundError):
					print('%s throwing errors' % (file))
					b.write(file)
					info = process_info()
					return

			#If doc file, convert to txt file using command line function "antiword", then use findkeywords
			elif file_extension == '.doc':
				newfile = workspace + file.replace('.doc','.txt')
				os.system("antiword '%s' > '%s'" % (file_path, newfile))
				getwords_doc = findkeywords(newfile)
				info = process_info()
				return 

			#No conversion necessary, use findkeywords
			elif file_extension == '.txt':
				getwords_txt = findkeywords(file_path)
				info = process_info()
				return 
				
			#Ignore any other files in directory, i.e. media files
			else:
				print('%s ignored' % file)
				ig.write(file)
				ig.write('\n')
				info = process_info()
				return
	except:
		print('%s BAD' % (file))
		fp = open(output + 'files_processed.txt','a')
		fp.write(file)
		fp.write('\n')
		fp.close()
		b = open(output + 'bad_files.txt','a')
		b.write(file)
		b.write('\n')
		b.close()
		return

##THIS IS THE START OF THE CODE, everything above this are functions.
if __name__ == "__main__":
	
	##Get folder and worker from user
	directory = os.getcwd()
	folder = input("What is the name of the folder where your files are contained? (This is how your input should be formatted: /folder/)\n")
	type(folder)

	#Set up regular expressions
	regex_rsid = re.compile(r'rs[0-9][0-9]*')
	regex_c = re.compile(r'\bc\..+')
	regex_p = re.compile(r'\bp\..+')
	regex_v = re.compile(r'\b[A-Z][0-9][0-9]*[A-Z]\b')
	regex_cdna = re.compile(r'\b[0-9][0-9]*[ATGC]>[ATGC]\b')

	#lists of amino acids and nucleotides for determining files that need to be manually reviewed

	nucleotides = ['A','C','G','T']
	amino_acids_1_letter = ['A','R','N','D','C','E','Q','G','H','I','L','K','M','F','P','S','T','W','Y','V']
	amino_acids_3_letter = ['Ala','Arg','Asn','Asp','Cys','Glu','Gln','Gly','His','Ile','Leu','Lys','Met','Phe','Pro','Ser','Thr','Trp','Tyr','Val']

	# #Iterate through files in folder, store files in list
	file_list = os.listdir(directory + folder)
	file_list_size = len(file_list)

	#Create folders where things will be stored in organized manner
	high_priority = directory + '/%s_prioritized/high_priority/' % (folder.strip('/'))
	workspace = directory + '/%s_workspace/' % (folder.strip('/'))
	output = directory + '/%s_prioritized/' % (folder.strip('/'))

	os.makedirs(high_priority)
	os.makedirs(workspace)

	process_time = open(output + 'process_time.txt','a')
	process_header = 'Filename' + '\t' + 'File Size (bytes)' + '\t' + 'Process Time (seconds)' + '\n'
	process_time.write(process_header)
	process_time.close()

	##Use parallel processing to run masterfunction against file list using all cores!!!
	##This returns a list of dataframes from all the files and then concatenates them all together to create a master dataframe
	with concurrent.futures.ProcessPoolExecutor() as executor:
		x = executor.map(masterfunction,file_list)