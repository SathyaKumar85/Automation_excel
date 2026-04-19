import numpy as np
np.float = float
import pandas as pd
import os
import glob
from tqdm import tqdm

def get_excel_column_letter(n):
    name = ""
    n += 1 
    while n > 0:
        n, remainder = divmod(n - 1, 26)
        name = chr(65 + remainder) + name
    return name

def generate_file_map(folder_path, sheet_name, anchor_keyword, target_column):
    output_name = "Column_Mapping_Output.xlsx"
    summary_list = []
    error_list = []
    
    existing_files = []
    old_summary_df = pd.DataFrame()
    old_error_df = pd.DataFrame()

    if os.path.exists(output_name):
        try:
            old_summary_df = pd.read_excel(output_name, sheet_name="Processed")
            old_error_df = pd.read_excel(output_name, sheet_name="Errors")
            # Create a list of filenames we already have so we can skip them
            existing_files = old_summary_df["File Name"].tolist() + old_error_df["File Name"].tolist()
            print(f"Found existing output. Skipping {len(existing_files)} files already processed.")
        except Exception:
            print("Output file found but could not be read. Creating fresh report.")

    search_pattern = os.path.join(folder_path, "*.xl*")
    all_files = glob.glob(search_pattern)
    
    file_list = [f for f in all_files if os.path.basename(f) not in existing_files]

    if not file_list:
        print("No new files found to process.")
        return

    print(f"Processing {len(file_list)} new files...")

    for file_path in tqdm(file_list, desc="Batch Progress"):
        file_name = os.path.basename(file_path)
        
        if output_name in file_name:
            continue

        try:
            xl = pd.ExcelFile(file_path)
            if sheet_name not in xl.sheet_names:
                error_list.append({"File Name": file_name, "Error": "Sheet not found"})
                continue

            df_raw = pd.read_excel(file_path, sheet_name=sheet_name, header=None)
            found_data = False

            for i, row in df_raw.iterrows():
                row_as_list = [str(val).strip() for val in row.tolist()]
                
                if anchor_keyword in row_as_list:
                    try:
                        col_idx = row_as_list.index(target_column)
                        letter = get_excel_column_letter(col_idx)
                        
                        summary_list.append({
                            "File Name": file_name,
                            "Target Column": target_column,
                            "Excel Column letter": f"Column {letter}"
                        })
                        found_data = True
                        break 
                    except ValueError:
                        continue

            if not found_data:
                error_list.append({"File Name": file_name, "Error": "Target column not found"})

        except Exception as e:
            error_list.append({"File Name": file_name, "Error": str(e)})

    new_summary_df = pd.DataFrame(summary_list)
    new_error_df = pd.DataFrame(error_list)

    final_summary = pd.concat([old_summary_df, new_summary_df], ignore_index=True)
    final_error = pd.concat([old_error_df, new_error_df], ignore_index=True)

    with pd.ExcelWriter(output_name, engine='openpyxl') as writer:
        final_summary.to_excel(writer, sheet_name="Processed", index=False)
        if not final_error.empty:
            final_error.to_excel(writer, sheet_name="Errors", index=False)

    print(f"\nBatch complete! Total files in report: {len(final_summary) + len(final_error)}")
FOLDER = r"Path"
generate_file_map(FOLDER, "sheetname", "anchorword", "targetcolumn")