"""
Author: David Chang
This script iterates through supplemental data files and extracts gene-variant associations.
"""

##Get packages needed

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
import cProfile, pstats, io
import datetime

#For logging info about file processing. Gets processing time and stores this information in txt file. Also keeps record of every file completed.
#Master function that is executed against the entire file list
def masterfunction(file):
	try:
		#start time
		time_initial = time.time()
		pmid = re.sub(r'_.*','',file)
		file_amount = len([s for s in file_list if str(pmid) in s])

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

			except (FileNotFoundError,UnicodeError,UnicodeDecodeError,AssertionError):
				print('%s cannot be opened' % (file))
				b.write(file)
				b.write('\n')
				return

			#initialize variables
			wc_genes = 0
			wc_rsids = 0
			wc_c = 0
			wc_p = 0
			wc_v = 0
			wc_cDNA = 0
			
			filename_list = []
			pmid_list = []
			gene_list = []
			rsid_list = []
			c_list = []
			p_list = []
			v_list = []
			cdna_list = []
			date_list = []
			worker_list = []
			file_number = []
			nucleotides_list = []
			aa_1_letter_list = []
			aa_3_letter_list = []

			#Search for variants line by line and word by word in the file
			for line in lines:
				line = line.replace('\n','')
				gene_inner = []
				rsid_inner = []
				c_inner = []
				p_inner = []
				v_inner = []
				cdna_inner = []
				date_list =[]
				worker_list = []
				file_number = []

				#For all matches in each line, store them in a list that will be appended to a larger list. 
				#This is to things in tact with the line they came from.
				for item in line.split():
					rsid_match = re.search(regex_rsid,item)
					c_match = re.search(regex_c, item)
					p_match = re.search(regex_p,item)
					v_match = re.search(regex_v,item)
					cdna_match = re.search(regex_cdna,item)
					if rsid_match:
						rsid_inner.append(rsid_match.group())
						wc_rsids += 1
					elif c_match:
						c_inner.append(c_match.group())
						wc_c += 1
					elif p_match:
						p_inner.append(p_match.group())
						wc_p += 1
					elif v_match:
						v_inner.append(v_match.group())
						wc_v += 1
					elif cdna_match:
						cdna_inner.append(cdna_match.group())
						wc_cDNA += 1
					for gene in genelist:
						if gene in item:
							gene_inner.append(gene)
							wc_genes += 1
					if item in nucleotides:
						nucleotides_list.append(item)
					if item in amino_acids_1_letter:
						aa_1_letter_list.append(item)
					elif item in amino_acids_3_letter:
						aa_3_letter_list.append(item)
				#Append the inner lists to larger lists. Every time there are no variants contained in a line from the file, a blank is appended.
				#This is to ensure that all lists are the same length, which is necessary to create a dataframe
				if len(gene_inner) != 0:
					x = ', '.join(gene_inner)
					gene_list.append(x)
				else:
					gene_list.append('')
				if len(rsid_inner) != 0:
					x = ', '.join(rsid_inner)
					rsid_list.append(x)
				else:
					rsid_list.append('')
				if len(c_inner) != 0:
					x = ', '.join(c_inner)
					c_list.append(x)
				else:
					c_list.append('')
				if len(p_inner) != 0:
					x = ', '.join(p_inner)
					p_list.append(x)
				else:
					p_list.append('')
				if len(v_inner) != 0:
					x = ', '.join(v_inner)
					v_list.append(x)
				else:
					v_list.append('')
				if len(cdna_inner) != 0:
					x = ', '.join(cdna_inner)
					cdna_list.append(x)
				else:
					cdna_list.append('')
				
				#If the entire line from the file contains no variants, the last item in the variant lists that was just appended will be blank.
				#If this is the case, delete that row from the dataframe.
				if (rsid_list[-1] == '' and c_list[-1] == '' and p_list[-1] == '' and v_list[-1] == '' and cdna_list[-1] == ''):
					del gene_list[-1]
					del rsid_list[-1]
					del c_list[-1]
					del p_list[-1]
					del v_list[-1]
					del cdna_list[-1]

			nucleotides_count = len(nucleotides_list)
			aa_1_count = len(aa_1_letter_list)
			aa_3_count = len(aa_3_letter_list)

			variant_str = file + '\t' + str(wc_genes) + '\t' + str(wc_rsids) + '\t' + str(wc_c) + '\t' + str(wc_p) + '\t' + str(wc_v) + '\t' + str(wc_cDNA) + '\t'+ str(nucleotides_count) + '\t' + str(aa_1_count) + '\t' + str(aa_3_count) + '\n'
			vc.write(variant_str)

			#If file contains no variants, then check to see if it contains amino acids or nucleotides.
			#If it does, it will be moved to another folder for manual review. If not, then nothing will happen.

			if (wc_rsids == 0 and wc_c == 0 and wc_p == 0 and wc_v == 0 and wc_cDNA == 0):
				print('No variants found!!!')
				if (nucleotides_count != 0 or aa_1_count != 0 or aa_3_count != 0):
					print('Manual file!')
					unique_nucleotides_count = str(len(set(nucleotides_list)))
					unique_aa_1_count = str(len(set(aa_1_letter_list)))
					unique_aa_3_count = str(len(set(aa_3_letter_list)))
					manual_str = file + '\t'+ str(nucleotides_count) +'\t' + str(aa_1_count) + '\t' + str(aa_3_count) + '\t' + unique_nucleotides_count + '\t' + unique_aa_1_count + '\t' + unique_aa_3_count + '\n'
					m.write(manual_str)
					shutil.copy(file_path, manual)
					return
				else:
					print('%s contains nothing!' % (file))
					b.write(file)
					b.write('\n')
					return
			##If the file contains data, then return a dataframe and output the dataframe as a .txt file. 
			elif (wc_rsids != 0 or wc_c != 0 or wc_p != 0 or wc_v != 0 or wc_cDNA != 0):
				print('%s contains data!' % (file))
				g.write(file)
				g.write('\n')

				for i in range(0,len(gene_list)):
					filename_list.append(file)
					pmid_list.append(pmid)
					date_list.append(date)
					worker_list.append(worker)
					file_number.append(file_amount)

				d = {'PMID': pmid_list,'Date of Download': date_list, 'Worker Name': worker_list,'Number of files': file_number,'Filename': filename_list, 'Gene': gene_list, 'rsID': rsid_list, 'c.': c_list, 'p.': p_list, 'protein': v_list, 'cDNA': cdna_list}
				df = pd.DataFrame(data=d)

				#Restructure dataframe in correct column order
				df = df[['PMID','Date of Download','Worker Name','Number of files','Filename','Gene','rsID','c.','p.','protein','cDNA']]

				text_file = file

				if file_extension == '.pdf':
					text_file = file.replace('.pdf','.txt')
				elif file_extension == '.doc':
					text_file = file.replace('.doc','.txt')

				output_loc = dataframes + text_file

				df.to_csv(output_loc,sep='\t')

				return df		

		#Behaves nearly identical to findkeywords, except that function iterates row by row and column by column, scanning every cell
		def findkeywords_excel(filepath):
			try:
				book = open_workbook(filepath)
			except (xlrd.biffh.XLRDError,IndexError,KeyError,UnicodeError,UnicodeDecodeError,BadZipfile,AssertionError,FileNotFoundError):
				print('%s cannot be opened' % (file))
				b.write(file)
				b.write('\n')
				return

			# #Initialize variables

			wc_genes = 0
			wc_rsids = 0
			wc_c = 0
			wc_p = 0
			wc_v = 0
			wc_cDNA = 0

			filename_list = []
			pmid_list = []
			gene_list = []
			rsid_list = []
			c_list = []
			p_list = []
			v_list = []
			cdna_list = []
			date_list = []
			worker_list = []
			file_number = []
			nucleotides_list = []
			aa_1_letter_list = []
			aa_3_letter_list = []

			for number in range(book.nsheets):
				sheet = book.sheet_by_index(number)
				for i in range(sheet.nrows):
					gene_inner = []
					rsid_inner = []
					p_inner = []
					c_inner = []
					v_inner = []
					cdna_inner = []
					row = sheet.row_values(i)
					for cell in row:
						cell_str = str(cell)
						gene_match = cell_str in genelist
						rsid_matches = re.findall(regex_rsid,cell_str)
						c_matches = re.findall(regex_c,cell_str)
						p_matches = re.findall(regex_p,cell_str)
						v_matches = re.findall(regex_v,cell_str)
						cdna_matches = re.findall(regex_cdna,cell_str)

						if gene_match:
							gene_inner.append(cell_str)
							wc_genes += 1
						elif len(rsid_matches) != 0:
							rsid_matches = ', '.join(rsid_matches)
							rsid_inner.append(rsid_matches)
							wc_rsids += 1
						elif len(c_matches) != 0:
							c_matches = ', '.join(c_matches)
							c_inner.append(c_matches)
							wc_c += 1
						elif len(p_matches) != 0:
							p_matches = ', '.join(p_matches)
							p_inner.append(p_matches)
							wc_p += 1
						elif len(v_matches) != 0:
							v_matches = ', '.join(v_matches)
							v_inner.append(v_matches)
							wc_v += 1
						elif len(cdna_matches) != 0:
							cdna_matches = ', '.join(cdna_matches)
							cdna_inner.append(cdna_matches)
							wc_cDNA += 1
						if cell_str in nucleotides:
							nucleotides_list.append(cell_str)
						if cell_str in amino_acids_1_letter:
							aa_1_letter_list.append(cell_str)
						elif cell_str in amino_acids_3_letter:
							aa_3_letter_list.append(cell_str)
		
					if len(gene_inner) != 0:
						x = ', '.join(gene_inner)
						gene_list.append(x)
					else:
						gene_list.append('')
					if len(rsid_inner) != 0:
						x = ', '.join(rsid_inner)
						rsid_list.append(x)
					else:
						rsid_list.append('')
					if len(c_inner) != 0:
						x = ', '.join(c_inner)
						c_list.append(x)
					else:
						c_list.append('')
					if len(p_inner) != 0:
						x = ', '.join(p_inner)
						p_list.append(x)
					else:
						p_list.append('')
					if len(v_inner) != 0:
						x = ', '.join(v_inner)
						v_list.append(x)
					else:
						v_list.append('')
					if len(cdna_inner) != 0:
						x = ', '.join(cdna_inner)
						cdna_list.append(x)
					else:
						cdna_list.append('')

					if (rsid_list[-1] == '' and c_list[-1] == '' and p_list[-1] == '' and v_list[-1] == '' and cdna_list[-1] == ''):
						del gene_list[-1]
						del rsid_list[-1]
						del c_list[-1]
						del p_list[-1]
						del v_list[-1]
						del cdna_list[-1]

			nucleotides_count = len(nucleotides_list)
			aa_1_count = len(aa_1_letter_list)
			aa_3_count = len(aa_3_letter_list)

			variant_str = file + '\t' + str(wc_genes) + '\t' + str(wc_rsids) + '\t' + str(wc_c) + '\t' + str(wc_p) + '\t' + str(wc_v) + '\t' + str(wc_cDNA) + '\t'+ str(nucleotides_count) + '\t' + str(aa_1_count) + '\t' + str(aa_3_count) + '\n'
			vc.write(variant_str)

			if (wc_rsids == 0 and wc_c == 0 and wc_p == 0 and wc_v == 0 and wc_cDNA == 0):
				print('No variants found!!!')
				if (nucleotides_count != 0 or aa_1_count != 0 or aa_3_count != 0):
					print('Manual file!')
					unique_nucleotides_count = str(len(set(nucleotides_list)))
					unique_aa_1_count = str(len(set(aa_1_letter_list)))
					unique_aa_3_count = str(len(set(aa_3_letter_list)))
					manual_str = file + '\t'+ str(nucleotides_count) +'\t' + str(aa_1_count) + '\t' + str(aa_3_count) + '\t' + unique_nucleotides_count + '\t' + unique_aa_1_count + '\t' + unique_aa_3_count + '\n'
					m.write(manual_str)
					shutil.copy(file_path, manual)
					return
				else:
					print('%s contains nothing!' % (file))
					b.write(file)
					b.write('\n')
					return
			elif (wc_rsids != 0 or wc_c != 0 or wc_p != 0 or wc_v != 0 or wc_cDNA != 0):
				print('%s contains data!' % (file))
				g.write(file)
				g.write('\n')

				for i in range(0,len(gene_list)):
					filename_list.append(file)
					pmid_list.append(pmid)
					date_list.append(date)
					worker_list.append(worker)
					file_number.append(file_amount)	

				d = {'PMID': pmid_list,'Date of Download': date_list, 'Worker Name': worker_list,'Number of files': file_number,'Filename': filename_list, 'Gene': gene_list, 'rsID': rsid_list, 'c.': c_list, 'p.': p_list, 'protein': v_list, 'cDNA': cdna_list}
				df = pd.DataFrame(data=d)

				#Restructure dataframe in correct column order
				df = df[['PMID','Date of Download','Worker Name','Number of files','Filename','Gene','rsID','c.','p.','protein','cDNA']]

				newfilepath = file

				if file_extension == '.xls':
					newfilepath = file.replace('.xls','.txt')
				elif file_extension == '.xlsx':
					newfilepath = file.replace('.xlsx','.txt')
				elif file_extension == '.docx':
					newfilepath = file.replace('.docx','.txt')

				
				output_loc = dataframes + newfilepath

				df.to_csv(output_loc,sep='\t')

				return df

		#Behaves exactly the same as findkeywords_excel. A few different functions/variables are used because of different module.
		def findkeywords_csv(filepath):
		# #Initialize variables

			wc_genes = 0
			wc_rsids = 0
			wc_c = 0
			wc_p = 0
			wc_v = 0
			wc_cDNA = 0

			pmid_list = []
			filename_list = []
			gene_list = []
			rsid_list = []
			c_list = []
			p_list = []
			v_list = []
			cdna_list = []
			date_list = []
			worker_list = []
			file_number = []
			nucleotides_list = []
			aa_1_letter_list = []
			aa_3_letter_list = []

			if file_extension == '.tsv':
				try:
					tsvin = open(filepath,newline = '')
					reader = csv.reader(tsvin,delimiter = '\t')
				except (csv.Error,UnicodeError,UnicodeDecodeError,IndexError,KeyError,AssertionError,FileNotFoundError):
					print('%s cannot be opened' % (file))
					b.write(file)
					b.write('\n')
					return
			else:
				try:
					csvin = open(filepath,newline = '')
					reader = csv.reader(csvin)
				except (csv.Error,UnicodeError,UnicodeDecodeError,IndexError,KeyError,AssertionError,FileNotFoundError):
					print('%s cannot be opened' % (file))
					b.write(file)
					b.write('\n')
					return
			try:
				for row in reader:
					gene_inner = []
					rsid_inner = []
					c_inner = []
					p_inner = []
					v_inner = []
					cdna_inner = []
					for cell in row:
						gene_match = cell in genelist
						rsid_matches = re.findall(regex_rsid,cell)
						c_matches = re.findall(regex_c,cell)
						p_matches = re.findall(regex_p,cell)
						v_matches = re.findall(regex_v,cell)
						cdna_matches = re.findall(regex_cdna,cell)

						if gene_match:
							gene_inner.append(cell)
							wc_genes += 1
						elif len(rsid_matches) != 0:
							rsid_matches = ', '.join(rsid_matches)
							rsid_inner.append(rsid_matches)
							wc_rsids += 1
						elif len(c_matches) != 0:
							c_matches = ', '.join(c_matches)
							c_inner.append(c_matches)
							wc_c += 1
						elif len(p_matches) != 0:
							p_matches = ', '.join(p_matches)
							p_inner.append(p_matches)
							wc_p += 1
						elif len(v_matches) != 0:
							v_matches = ', '.join(v_matches)
							v_inner.append(v_matches)
							wc_v += 1
						elif len(cdna_matches) != 0:
							cdna_matches = ', '.join(cdna_matches)
							cdna_inner.append(cdna_matches)
							wc_cDNA += 1
						if cell in nucleotides:
							nucleotides_list.append(cell_str)
						if cell in amino_acids_1_letter:
							aa_1_letter_list.append(cell_str)
						elif cell in amino_acids_3_letter:
							aa_3_letter_list.append(cell_str)
		
					if len(gene_inner) != 0:
						x = ', '.join(gene_inner)
						gene_list.append(x)
					else:
						gene_list.append('')
					if len(rsid_inner) != 0:
						x = ', '.join(rsid_inner)
						rsid_list.append(x)
					else:
						rsid_list.append('')
					if len(c_inner) != 0:
						x = ', '.join(c_inner)
						c_list.append(x)
					else:
						c_list.append('')
					if len(p_inner) != 0:
						x = ', '.join(p_inner)
						p_list.append(x)
					else:
						p_list.append('')
					if len(v_inner) != 0:
						x = ', '.join(v_inner)
						v_list.append(x)
					else:
						v_list.append('')
					if len(cdna_inner) != 0:
						x = ', '.join(cdna_inner)
						cdna_list.append(x)
					else:
						cdna_list.append('')

					if (rsid_list[-1] == '' and c_list[-1] == '' and p_list[-1] == '' and v_list[-1] == '' and cdna_list[-1] == ''):
						del gene_list[-1]
						del rsid_list[-1]
						del c_list[-1]
						del p_list[-1]
						del v_list[-1]
						del cdna_list[-1]

				nucleotides_count = len(nucleotides_list)
				aa_1_count = len(aa_1_letter_list)
				aa_3_count = len(aa_3_letter_list)

				variant_str = file + '\t' + str(wc_genes) + '\t' + str(wc_rsids) + '\t' + str(wc_c) + '\t' + str(wc_p) + '\t' + str(wc_v) + '\t' + str(wc_cDNA) + '\t'+ str(nucleotides_count) + '\t' + str(aa_1_count) + '\t' + str(aa_3_count) + '\n'
				vc.write(variant_str)

				if (wc_rsids == 0 and wc_c == 0 and wc_p == 0 and wc_v == 0 and wc_cDNA == 0):
					print('No variants found!!!')
					if (nucleotides_count != 0 or aa_1_count != 0 or aa_3_count != 0):
						print('Manual file!')
						unique_nucleotides_count = str(len(set(nucleotides_list)))
						unique_aa_1_count = str(len(set(aa_1_letter_list)))
						unique_aa_3_count = str(len(set(aa_3_letter_list)))
						manual_str = file + '\t'+ str(nucleotides_count) +'\t' + str(aa_1_count) + '\t' + str(aa_3_count) + '\t' + unique_nucleotides_count + '\t' + unique_aa_1_count + '\t' + unique_aa_3_count + '\n'
						m.write(manual_str)
						shutil.copy(file_path, manual)
					else:
						print('%s contains nothing!' % (file))
						b.write(file)
						b.write('\n')
						return

				elif (wc_rsids != 0 or wc_c != 0 or wc_p != 0 or wc_v != 0 or wc_cDNA != 0):
					print('%s contains data!' % (file))
					g.write(file)
					g.write('\n')

					for i in range(0,len(gene_list)):
						filename_list.append(file)
						pmid_list.append(pmid)
						date_list.append(date)
						worker_list.append(worker)
						file_number.append(file_amount)

					d = {'PMID': pmid_list,'Date of Download': date_list, 'Worker Name': worker_list,'Number of files': file_number,'Filename': filename_list, 'Gene': gene_list, 'rsID': rsid_list, 'c.': c_list, 'p.': p_list, 'protein': v_list, 'cDNA': cdna_list}
					df = pd.DataFrame(data=d)

				#Restructure dataframe in correct column order
					df = df[['PMID','Date of Download','Worker Name','Number of files','Filename','Gene','rsID','c.','p.','protein','cDNA']]
					
					newfilepath = file

					if file_extension == '.csv':
						newfilepath = file.replace('.csv','.txt')
					elif file_extension == '.tsv':
						newfilepath = file.replace('.tsv','.txt')

					output_loc = dataframes + newfilepath

					df.to_csv(output_loc,sep='\t')

					if file_extension == '.csv':
						csvin.close()
					elif file_extension == 'tsv':
						tsvin.close()

					return df

			except (UnicodeError,UnicodeDecodeError,KeyError,IndexError):
				b.write(file)
				b.write('\n')
				print('Something is really messed up with this file')
				return

		##Main body of masterfunction, in which it calls other functions. This is essentially in a loop and used on every file.
		
		#Text Files are created for storing information about the files that are processed. These are all located in the "output" folder
		with open(output + 'files_processed.txt','a') as fp, open(output + 'manual.txt','a') as m, open(output + 'good_files.txt','a') as g, open(output + 'bad_files.txt','a') as b, open(output + 'files_ignored.txt','a') as ig, open(output + 'variant_counts.txt','a') as vc, open(output + 'process_time.txt','a') as pt, open(output + 'big_files_manual.txt','a') as bfm:
			file_index = file_list.index(file)
			file_str = file + '\t' + "%s/%s" % (file_index,file_list_size) + '\n'
			print("File Index: %s/%s" % (file_index,file_list_size))
			print('File: %s' % (file))
			file_path = directory + folder + file
			file_size = os.path.getsize(file_path)
			file_extension = os.path.splitext(file)[1]

			##Copy all files greater than 20 MB out of the file folder and into another folder called big_files_manual for manual processing
			##These files tend to take a long time to process, so they can be processed faster via manual review.

			if file_size > 20000000 and file_extension != '.zip' and file_extension != '.rar' and file_extension != '.tar':
				print('%s too big. Requires manual review!' % (file))
				info = process_info()
				bfm.write(file)
				bfm.write('\n')
				shutil.copy(file_path, big_manual)
				return

			else:
				#If excel file, use findkeywords_excel, which uses xlrd module.
				if file_extension == '.xlsx' or file_extension == '.xls':
					getexcel = findkeywords_excel(file_path)
					info = process_info()
					return getexcel

				##If csv or tsv file, use findkeywords_csv, which uses csv module.
				elif file_extension == '.csv' or file_extension == '.tsv':
					getcsv = findkeywords_csv(file_path)
					info = process_info()
					return getcsv

				#If pdf, convert to txt file using command line function "pdftotext", and use findkeywords
				elif file_extension == '.pdf':
					newfilepath = workspace + file.replace('.pdf','.txt')
					os.system("pdftotext -layout '%s' '%s'" % (file_path, newfilepath))
					getwords_pdf = findkeywords(newfilepath)
					info = process_info()
					return getwords_pdf

				#If file_extension contains '&type=printable', this is a pdf. Convert to txt file using command line function "pdftotext", and use findkeywords
				elif '&type=printable' in file_extension:
					newfilepath = workspace + re.sub(r'\..*','.txt',file)
					os.system("pdftotext -layout '%s' '%s'" % (file_path, newfilepath))
					getwords_pdf = findkeywords(newfilepath)
					info = process_info()
					return getwords_pdf

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
					return getwords_doc

				#No conversion necessary, use findkeywords
				elif file_extension == '.txt':
					getwords_txt = findkeywords(file_path)
					info = process_info()
					return getwords_txt
					
				#Ignore any other files in directory, i.e. media files
				else:
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
	
	directory = os.getcwd()

	##Get folder containing files for processing and worker name from user
	folder = input("What is the name of the folder where your files are contained? (This is how your input should be formatted: /folder/)\n")
	type(folder)

	worker = input("What is your name?\n")
	type(worker)

	#Get today's date
	date = datetime.datetime.today().strftime('%Y-%m-%d')

	#Get list of genes
	genelist = [] 

	with open('genelist.txt') as g:  
		for cnt,line in enumerate(g):
			x = line.strip('\n')
			genelist.append(x)

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

	#Iterate through files in folder, store files in list
	file_list = os.listdir(directory + folder)
	file_list_size = len(file_list)

	#Create folders where things will be stored in organized manner
	dataframes = directory + '/%s_results/dataframes/' % (folder.strip('/'))
	manual = directory + '/%s_results/manual/' % (folder.strip('/'))
	big_manual = directory + '/%s_results/big_files_manual/' % (folder.strip('/'))
	output = directory + '/%s_results/output/' % (folder.strip('/'))
	workspace = directory + '/%s_workspace/' % (folder.strip('/'))

	os.makedirs(dataframes)
	os.makedirs(manual)
	os.makedirs(big_manual)
	os.makedirs(output)
	os.makedirs(workspace)

	#Create headers for manual.txt and variant_counts.txt for data logging
	manual_file = open(output + 'manual.txt','a')
	manual_header = 'Filename' + '\t' + 'Nucleotides' + '\t' + 'Amino Acids (1 letter)' + '\t' + 'Amino Acids (3 letter)' + '\t' + 'Unique Nucleotides' + '\t' + 'Unique Amino Acids (1 letter)' + '\t' + 'Unique Amino Acids (3 letter)' + '\n'
	manual_file.write(manual_header)
	manual_file.close()
	variant_count = open(output + 'variant_counts.txt','a')
	variant_header = 'Filename' + '\t' + 'Gene' + '\t' + 'rsID' + '\t' + 'c.' + '\t' + 'p.' + '\t' + 'protein' + '\t' + 'cDNA' + '\t' + 'Nucleotides' + '\t' + 'Amino Acids (1 letter)' + '\t' + 'Amino Acids (3 letter)' + '\n'
	variant_count.write(variant_header)
	variant_count.close()
	process_time = open(output + 'process_time.txt','a')
	process_header = 'Filename' + '\t' + 'File Size (bytes)' + '\t' + 'Process Time (seconds)' + '\n'
	process_time.write(process_header)
	process_time.close()

	#Use parallel processing to run masterfunction against file list using all cores!!!
	#This returns a list of dataframes from all the files and then concatenates them all together to create a master dataframe
	with concurrent.futures.ProcessPoolExecutor() as executor:
		master_df = pd.concat(executor.map(masterfunction,file_list))

	#Create master files in the forms of .xlsx, .csv, and .txt containing all information at the end
	#NOTE: .xlsx file will likely not contain all information as there is a row limit to the file. .txt file will be best file for post processing.

	masterlist_xlsx = output + '%s_masterlist.xlsx' % (folder.strip('/'))
	masterlist_csv = output + '%s_masterlist.csv' % (folder.strip('/'))
	masterlist_txt = output + '%s_masterlist.txt' % (folder.strip('/'))
	writer = pd.ExcelWriter(masterlist_xlsx,engine = 'xlsxwriter')
	master_df.to_excel(writer,'Sheet1')
	master_df.to_csv(masterlist_csv)
	master_df.to_csv(masterlist_txt,sep='\t')
	writer.save()