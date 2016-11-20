from PIL import Image
import os
import numpy as np
from shutil import copyfile
import pdb
from tqdm import tqdm #progress bar
import sys


def _traverse_folders(root_folder,output_folder,wanted_files):
	'''
	function to traverse a folder structure and run a function in each folder
	'''
	for folder in tqdm(os.listdir(root_folder)):
		print("Processing: {}".format(folder))
		if folder.startswith("MS-DAR-"):
			_copy_jpg_on_filename(folder,output_folder,wanted_files)

def _traverse_folders_write(root_folder):
	'''
	function to traverse a folder structure and run a function in each folder
	'''
	for folder in tqdm(os.listdir(root_folder)):
		print("Processing: {}".format(folder))
		if folder.startswith("MS-DAR-"):
			_save_filename_on_dim(folder)

def _copy_jpg_on_dim(root_folder,output_folder):
	'''
	function to 1) get average dimensions for all images in a folder
				2) copy all images less than average to a target folder
	'''
	if root_folder.startswith("MS-DAR-"):
		print("Processing: {}".format(root_folder))
		sizes = []
		prev_num = 0
		for file in os.listdir(root_folder):
			if file.endswith(".jpg"):
				im=Image.open(os.path.join(root_folder,file))
				sizes.append([im.size[0]*im.size[1],file])
		average_size = np.mean([i[0] for i in sizes])
		for i in sizes:
			if i[0] < average_size:
				copyfile(os.path.join(root_folder,i[1]),os.path.join(output_folder,i[1]))

def _copy_jpg_on_filename(root_folder,output_folder,wanted_files_txt):
	'''
	function to 1) copy all images from a textfile of desired images
	'''
	wanted_files = [line.rstrip('\n') for line in open(wanted_files_txt)]
	if root_folder.startswith("MS-DAR-"):
		print("Processing: {}".format(root_folder))
		sizes = []
		prev_num = 0
		for file in tqdm(os.listdir(root_folder)):
			if file in wanted_files:
				if not os.path.exists(os.path.join(output_folder,file)):
					copyfile(os.path.join(root_folder,file),os.path.join(output_folder,file))

def _traverse_folders_spark(root_folder,output_folder,wanted_files):
	'''
	spark function to traverse a folder structure and run a function in each folder
	'''
	rootFolderRDD = sc.parallelize(os.listdir(root_folder))
	#placeholderRDD = rootFolderRDD.map(lambda x: _copy_jpg_on_dim(x,output_folder)).collect()
	placeholderRDD = rootFolderRDD.map(lambda x: _copy_jpg_on_filename(x,output_folder,wanted_files)).collect()

if __name__ == "__main__":
	from pyspark import SparkContext
	sc = SparkContext()
	print("Executing as main program")
	_traverse_folders_spark(sys.argv[1],sys.argv[2],sys.argv[3])