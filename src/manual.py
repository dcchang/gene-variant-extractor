#For scanning manual files
import os
import shutil
import re
import pandas as pd
import xlrd
from xlrd import open_workbook
import csv
import time
import zipfile    
import docx
import docx2csv
import concurrent.futures
from docx2csv import extract_tables,extract

##Have multiple functions for different file types, just like supp data scanner

def file_scanner(file):
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
		nucleotides_list = []
		aa_1_letter_list = []
		aa_3_letter_list = []
		
		if file_extension == '.pdf' or '&type=printable' in file_extension or file_extension == '.doc' or file_extension == '.txt':
			input_file = open(filepath,'r')
			lines = input_file.readlines()
			input_file.close()
			for line in lines:
				for item in line.split():
					if item in nucleotides:
						nucleotides_list.append(item)
					if item in amino_acids_1_letter:
						aa_1_letter_list.append(item)
					elif item in amino_acids_3_letter:
						aa_3_letter_list.append(item)
		elif file_extension == '.xlsx' or file_extension == '.xls' or file_extension == '.docx':
			book = open_workbook(filepath)
			for number in range(book.nsheets):
				sheet = book.sheet_by_index(number)
				for i in range(sheet.nrows):
					row = sheet.row_values(i)
					for cell in row:
						cell_str = str(cell)
						if cell_str in nucleotides:
							nucleotides_list.append(cell)
						if cell_str in amino_acids_1_letter:
							aa_1_letter_list.append(cell)
						elif cell_str in amino_acids_3_letter:
							aa_3_letter_list.append(cell)
		elif file_extension == '.csv' or file_extension == '.tsv':
			if file_extension == '.tsv':
				tsvin = open(filepath,newline = '')
				reader = csv.reader(tsvin,delimiter = '\t')
			else:
				csvin = open(filepath,newline = '')
				reader = csv.reader(csvin)

			for row in reader:
				gene_inner = []
				rsid_inner = []
				c_inner = []
				p_inner = []
				v_inner = []
				cdna_inner = []
				for cell in row:
					if cell in nucleotides:
						nucleotides_list.append(cell)
					if cell in amino_acids_1_letter:
						aa_1_letter_list.append(cell)
					elif cell in amino_acids_3_letter:
						aa_3_letter_list.append(cell)

		nucleotides_count = str(len(nucleotides_list))
		aa_1_count = str(len(aa_1_letter_list))
		aa_3_count = str(len(aa_3_letter_list))
		unique_nucleotides_count = str(len(set(nucleotides_list)))
		unique_aa_1_count = str(len(set(aa_1_letter_list)))
		unique_aa_3_count = str(len(set(aa_3_letter_list)))

		data_str = file + '\t'+ nucleotides_count +'\t' + aa_1_count + '\t' + aa_3_count + '\t' + unique_nucleotides_count + '\t' + unique_aa_1_count + '\t' + unique_aa_3_count + '\n'
		m.write(data_str)
		return

	with open(results + 'files_processed.txt','a') as fp, open(results + 'manual_counts.txt','a') as m, open(results + 'process_time.txt','a') as pt, open(results + 'files_ignored.txt','a') as ig:
		file_index = file_list.index(file)
		print("File Index: %s/%s" % (file_index,file_list_size))
		print('File: %s' % (file))
		file_str = file + '\t' + "%s/%s" % (file_index,file_list_size) + '\n'
		file_path = directory + folder + file
		file_size = os.path.getsize(file_path)
		file_extension = os.path.splitext(file)[1]

		#If excel file, use findkeywords_excel, which uses xlrd module.
		if file_extension == '.xlsx' or file_extension == '.xls':
			getexcel = findkeywords(file_path)
			info = process_info()
			return 

		##If csv or tsv file, use findkeywords_csv, which uses csv module.
		elif file_extension == '.csv' or file_extension == '.tsv':
			getcsv = findkeywords(file_path)
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
		else:
			ig.write(file)
			ig.write('\n')

if __name__ == "__main__":
	directory = os.getcwd()

	folder = input("What is the name of the folder where your files are contained? (This is how your input should be formatted: /folder/)\n")
	type(folder)

	nucleotides = ['A','C','G','T']
	amino_acids_1_letter = ['A','R','N','D','C','E','Q','G','H','I','L','K','M','F','P','S','T','W','Y','V']
	amino_acids_3_letter = ['Ala','Arg','Asn','Asp','Cys','Glu','Gln','Gly','His','Ile','Leu','Lys','Met','Phe','Pro','Ser','Thr','Trp','Tyr','Val']
	file_list = os.listdir(directory + folder)
	file_list_size = len(file_list)

	workspace = directory + '/%s_workspace/' % (folder.strip('/'))
	results = directory + '/%s_results/' % (folder.strip('/'))
	os.makedirs(workspace)
	os.makedirs(results)

	manual_counts = open(results + 'manual_counts.txt','a')
	header = 'Filename' + '\t' + 'Nucleotides' + '\t' + 'Amino Acids (1 letter)' + '\t' + 'Amino Acids (3 letter)' + '\t' + 'Unique Nucleotides' + '\t' + 'Unique Amino Acids (1 letter)' + '\t' + 'Unique Amino Acids (3 letter)' + '\n'
	manual_counts.write(header)
	manual_counts.close()

	process_time = open(results + 'process_time.txt','a')
	process_header = 'Filename' + '\t' + 'File Size (bytes)' + '\t' + 'Process Time (seconds)' + '\n'
	process_time.write(process_header)
	process_time.close()

	with concurrent.futures.ProcessPoolExecutor() as executor:
		manual_scanner = executor.map(file_scanner,file_list)

	##For getting sums of all stuff from manual files
	##Total number of hits for each category 
	##Total number of individual elements that are unique just a number i.e 2/4,15/20, x/3 letter
	##6 numbers for each file, totals of each category and fraction of unique items for each category

	##Integrate into current script and run this over old files
	##if the file is a manual file, do more of the analysis

	# for amino_acid in amino_acids_1_letter:
		# amino_list_count = len(list(filter(lambda x: x == amino_acid, aa_1_letter_list)))
		# amino_str = amino_acid + '\t' amino_list_count
		# amino_acid_counts.write(amino_str)
