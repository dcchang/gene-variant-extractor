##For combining dataframes contained in a directory

import os
import pandas as pd
import xlrd
import concurrent.futures

def combinedataframes(file):
	file_path = directory + folder + file
	file_extension = file_extension = os.path.splitext(file)[1]
	try: 
		df = pd.read_csv(file_path,sep='\t')
		print(file)
		return df
	except pd.parser.CParserError: 
		print(file)
		print('BAD')
		return

if __name__ == "__main__":
	directory = os.getcwd()

	folder = input("What is the name of the folder where your dataframe files are contained? (This is how your input should be formatted: /folder/)\n")
	type(folder)

	file_list = os.listdir(directory + folder)
			
	with concurrent.futures.ProcessPoolExecutor() as executor:
		master_df = pd.concat(executor.map(combinedataframes,file_list),ignore_index = True)
		
	master_df = master_df[['PMID','Date of Download','Worker Name','Number of files','Filename','Gene','rsID','c.','p.','protein','cDNA']]

	output = directory + '/output/'

	x = directory.split('/')
	name = x[len(x) -1].replace('_results','')

	masterlist_xlsx = output + '%s_masterlist.xlsx' % (name)
	masterlist_csv = output + '%s_masterlist.csv' % (name)
	masterlist_txt = output + '%s_masterlist.txt' % (name)
	writer = pd.ExcelWriter(masterlist_xlsx,engine = 'xlsxwriter')
	master_df.to_excel(writer,'Sheet1')
	master_df.to_csv(masterlist_csv)
	master_df.to_csv(masterlist_txt,sep='\t')
	writer.save()