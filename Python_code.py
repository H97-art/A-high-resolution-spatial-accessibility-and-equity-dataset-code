import os
import pandas as pd
import geopandas as gpd
import numpy as np
import math
import re

# 01: Convert encoding of CSV files to UTF-8
def convert_encoding():
    """Convert encoding of CSV files to UTF-8 with BOM"""
    # Define input and output folders
    input_folder = r"H:\2025本基road2\a养老设施分级_递增"
    output_folder = r"H:\2025本基road2\a养老设施分级_递增"

    # Create output folder if it doesn't exist
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    # Define possible encoding formats
    possible_encodings = ["utf-8", "gbk", "gb2312", "latin1", "iso-8859-1"]

    # Iterate through all files in the input folder
    for filename in os.listdir(input_folder):
        if filename.endswith(".csv"):
            file_path = os.path.join(input_folder, filename)
            
            # Try to read the file with different encodings
            for encoding in possible_encodings:
                try:
                    df = pd.read_csv(file_path, encoding=encoding)
                    print(f"Successfully read file {filename} with encoding {encoding}")
                    break
                except UnicodeDecodeError:
                    print(f"Failed to read file {filename} with encoding {encoding}, trying next encoding...")
                except Exception as e:
                    print(f"Unable to read file {filename}: {e}")
                    break
            else:
                print(f"Unable to read file {filename}, all encoding attempts failed, skipping file.")
                continue
            
            # Save file as UTF-8 encoding with BOM
            output_file_path = os.path.join(output_folder, filename)
            df.to_csv(output_file_path, index=False, encoding="utf-8-sig")
            print(f"File {filename} successfully saved as UTF-8 encoding to {output_file_path}")

    print("All files processed.")

# Gaussian calculation function with configurable parameters
def gaussian_calculation(input_shp_path, csv1_path, csv2_path, output_csv_path, 
                         output_gauss_correction_path, distance_threshold):
    """
    Perform Gaussian calculation for accessibility analysis
    
    Parameters:
    input_shp_path: Path to input shapefile
    csv1_path: Path to first CSV file (facility data)
    csv2_path: Path to second CSV file (population data)
    output_csv_path: Path to output CSV for facility summary
    output_gauss_correction_path: Path to output CSV for Gaussian correction
    distance_threshold: Distance threshold for Gaussian calculation
    """
    # Check if file exists
    if not os.path.exists(input_shp_path):
        print(f"File does not exist: {input_shp_path}")
        return

    # Step 1: Read Shapefile
    gdf = gpd.read_file(input_shp_path)

    # Check column names
    print("Current dataframe column names:")
    print(gdf.columns)

    try:
        # Filter features with Total_Leng field less than distance_threshold
        filtered_gdf = gdf[gdf['Total_Leng'] < distance_threshold]
    except KeyError as e:
        print(f"Column name error: {e}")
        print("Please check if column names are correct.")
        return

    # Step 2: Add four new fields and ensure data type is float
    filtered_gdf['Guss'] = 0.0
    filtered_gdf['Guss_pop'] = 0.0
    filtered_gdf['供需比'] = 0.0
    filtered_gdf['供需修正'] = 0.0

    # Ensure field types are float
    filtered_gdf['Guss'] = filtered_gdf['Guss'].astype(float)
    filtered_gdf['Guss_pop'] = filtered_gdf['Guss_pop'].astype(float)
    filtered_gdf['供需比'] = filtered_gdf['供需比'].astype(float)
    filtered_gdf['供需修正'] = filtered_gdf['供需修正'].astype(float)

    # Step 3: Calculate Guss field
    filtered_gdf['Guss'] = (
        (np.exp(-0.5 * (filtered_gdf['Total_Leng'] / distance_threshold) ** 2) - np.exp(-0.5)) /
        (1 - np.exp(-0.5))
    )

    # Step 4: Merge with first CSV data (facility data)
    csv1_df = pd.read_csv(csv1_path)
    merged_gdf = filtered_gdf.merge(csv1_df, left_on='Destinatio', right_on='sheshi3', how='left')

    # Step 5: Merge with second CSV data (population data)
    csv2_df = pd.read_csv(csv2_path)
    merged_gdf = merged_gdf.merge(csv2_df, left_on='OriginID', right_on='OID_', how='left')

    # Step 6: Calculate Guss_pop field
    merged_gdf['Guss_pop'] = merged_gdf['Guss'] * merged_gdf['grid_code']

    # Step 7: Summarize Guss_pop by Destinatio
    grouped_df = merged_gdf.groupby('Destinatio', as_index=False)['Guss_pop'].sum()
    grouped_df.to_csv(output_csv_path, index=False)

    # Step 8: Merge summary results back to shapefile
    final_gdf = merged_gdf.merge(grouped_df, on='Destinatio', how='left', suffixes=('', '_s'))

    # Step 9: Calculate supply-demand ratio
    if '床位数' not in final_gdf.columns:
        raise KeyError("Column '床位数' does not exist in final_gdf")
    final_gdf['供需比'] = final_gdf['床位数'] / final_gdf['Guss_pop_s']

    # Step 10: Calculate supply-demand correction
    final_gdf['供需修正'] = final_gdf['Guss'] * final_gdf['供需比']

    # Step 11: Summarize supply-demand correction by OriginID
    gauss_correction_grouped = final_gdf.groupby('OriginID', as_index=False)['供需修正'].sum()

    # Ensure output directory exists
    output_dir = os.path.dirname(output_gauss_correction_path)
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Export summary results to CSV file
    gauss_correction_grouped.to_csv(output_gauss_correction_path, index=False)

    print("Processing completed!")

# 02: Gaussian calculation with 3000m threshold
def gaussian_calculation_3000():
    """Perform Gaussian calculation with 3000m threshold"""
    input_shp_path = r"H:\2025本基road2\m0.1路网计算\重庆3_d.shp"
    csv1_path = r"H:\2025本基road2\a养老设施分级_递增\重庆d.csv"
    csv2_path = r"H:\2025本基road2\a0.1非零点\重庆d_3.csv"
    output_csv_path = r"H:\2025本基road2\a高斯计算\a设施汇总\重庆1_3.csv"
    output_gauss_correction_path = r"H:\2025本基road2\a高斯计算\a高斯汇总\重庆1_3.csv"
    
    gaussian_calculation(input_shp_path, csv1_path, csv2_path, output_csv_path, 
                        output_gauss_correction_path, 3000)

# 03: Gaussian calculation with 5000m threshold
def gaussian_calculation_5000():
    """Perform Gaussian calculation with 5000m threshold"""
    input_shp_path = r"H:\2025本基road2\m0.1路网计算\重庆3_e.shp"
    csv1_path = r"H:\2025本基road2\a养老设施分级_递增\重庆e.csv"
    csv2_path = r"H:\2025本基road2\a0.1非零点\重庆d_3.csv"
    output_csv_path = r"H:\2025本基road2\a高斯计算\a设施汇总\重庆2_3.csv"
    output_gauss_correction_path = r"H:\2025本基road2\a高斯计算\a高斯汇总\重庆2_3.csv"
    
    gaussian_calculation(input_shp_path, csv1_path, csv2_path, output_csv_path, 
                        output_gauss_correction_path, 5000)

# 04: Gaussian calculation with 8000m threshold
def gaussian_calculation_8000():
    """Perform Gaussian calculation with 8000m threshold"""
    input_shp_path = r"H:\2025本基road2\m0.1路网计算\重庆3_f.shp"
    csv1_path = r"H:\2025本基road2\a养老设施分级_递增\重庆f.csv"
    csv2_path = r"H:\2025本基road2\a0.1非零点\重庆d_3.csv"
    output_csv_path = r"H:\2025本基road2\a高斯计算\a设施汇总\重庆3_3.csv"
    output_gauss_correction_path = r"H:\2025本基road2\a高斯计算\a高斯汇总\重庆3_3.csv"
    
    gaussian_calculation(input_shp_path, csv1_path, csv2_path, output_csv_path, 
                        output_gauss_correction_path, 8000)

# 05: Gaussian calculation with 10000m threshold
def gaussian_calculation_10000():
    """Perform Gaussian calculation with 10000m threshold"""
    input_shp_path = r"H:\2025本基road2\m0.1路网计算\重庆3_g.shp"
    csv1_path = r"H:\2025本基road2\a养老设施分级_递增\重庆g.csv"
    csv2_path = r"H:\2025本基road2\a0.1非零点\重庆d_3.csv"
    output_csv_path = r"H:\2025本基road2\a高斯计算\a设施汇总\重庆4_3.csv"
    output_gauss_correction_path = r"H:\2025本基road2\a高斯计算\a高斯汇总\重庆4_3.csv"
    
    gaussian_calculation(input_shp_path, csv1_path, csv2_path, output_csv_path, 
                        output_gauss_correction_path, 10000)

# 06: Calculate accessibility total
def calculate_accessibility_total():
    """Calculate total accessibility by summing multiple Gaussian correction results"""
    # Define input and output folder paths
    input_folder = r"H:\2025本基road2\a高斯计算\a汇总"
    output_folder = r"H:\2025本基road2\a高斯计算\a汇总_总和"

    # Ensure output folder exists
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    # Get all .csv files in input folder
    files = [f for f in os.listdir(input_folder) if f.endswith('.csv')]

    # Process each file
    for file in files:
        file_path = os.path.join(input_folder, file)
        try:
            # Read CSV file
            df = pd.read_csv(file_path)
            
            # Step 1: Add "keda" column at the end
            df['keda'] = 0  # Initialize to 0
            
            # Step 2: Calculate "keda" column value
            # Ensure relevant columns exist, create and fill with 0 if not
            required_columns = ['供需修正', '匹配结果1', '匹配结果2', '匹配结果3']
            for col in required_columns:
                if col not in df.columns:
                    df[col] = 0  # If column doesn't exist, create and fill with 0
            
            # Calculate "keda" column value, treating null values as 0
            df['keda'] = df['供需修正'].fillna(0) + df['匹配结果1'].fillna(0) + \
                         df['匹配结果2'].fillna(0) + df['匹配结果3'].fillna(0)
            
            # Step 4: Save processed file to output folder
            output_path = os.path.join(output_folder, file)
            df.to_csv(output_path, index=False)
            print(f"File {file} processed and saved to {output_path}")
        except Exception as e:
            print(f"Error processing file {file}: {e}")

    print("All files processed!")

# 07: Match files by filename
def match_files_by_name():
    """Match OD files with point files based on filename"""
    # Define folder paths
    folder_path_od = r"G:\2025本基road2\a高斯计算\a汇总_总和"
    folder_path_points = r"G:\2025本基road2\a0.1非零点"
    output_folder = r"G:\2025本基road2\a高斯计算\a可视化"

    # Ensure output folder exists
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    # Get all OD files
    od_files = [f for f in os.listdir(folder_path_od) if f.endswith('.csv')]

    # Process each OD file
    for od_file in od_files:
        od_file_path = os.path.join(folder_path_od, od_file)
        
        # Read OD file
        od_df = pd.read_csv(od_file_path, encoding='utf-8')  # Adjust based on file encoding

        # Extract Chinese part from current OD filename
        chinese_part = ''.join(re.findall(r'[\u4e00-\u9fff]', od_file))

        # Get all files in points folder
        points_files = os.listdir(folder_path_points)

        # Find matching file in points folder
        for points_file in points_files:
            if points_file.endswith(".csv") and ''.join(re.findall(r'[\u4e00-\u9fff]', points_file)) == chinese_part:
                points_file_path = os.path.join(folder_path_points, points_file)
                points_df = pd.read_csv(points_file_path, encoding='utf-8')  # Adjust based on file encoding

                # Match based on OriginID and pointid columns
                merged_df = pd.merge(points_df, od_df[['OriginID', 'keda']], 
                                    left_on='OID_', right_on='OriginID', how='left')

                # Save result to specified folder
                output_file_path = os.path.join(output_folder, f"merged_{points_file}")
                merged_df.to_csv(output_file_path, index=False, encoding='utf-8')  # Adjust encoding as needed
                print(f"File {points_file} processed and saved as {output_file_path}")
                break  # Break after finding match

# Main execution
if __name__ == "__main__":
    # Execute all steps in sequence
    print("Starting encoding conversion...")
    convert_encoding()
    
    print("Starting Gaussian calculations...")
    print("Calculating with 3000m threshold...")
    gaussian_calculation_3000()
    
    print("Calculating with 5000m threshold...")
    gaussian_calculation_5000()
    
    print("Calculating with 8000m threshold...")
    gaussian_calculation_8000()
    
    print("Calculating with 10000m threshold...")
    gaussian_calculation_10000()
    
    print("Calculating accessibility totals...")
    calculate_accessibility_total()
    
    print("Matching files by name...")
    match_files_by_name()
    
    print("All processing completed!")
