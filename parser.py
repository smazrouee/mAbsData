import csv
import os
import json


# Convert CSV into a list of dictionaries
def read_csv(file: str, delim: str):
    info = []
    with open(file, encoding="utf-8-sig") as csv_file:
        reader = csv.reader(csv_file, delimiter=delim)
        categories = []
        for i, row in enumerate(reader):
            if len(row) == 0:  # Skip empty rows
                continue
            if i == 0:  # First row is the header
                categories = [col.strip().lower() for col in row]  
            else:  # Process data rows
                if len(row) != len(categories):  
                    print(f"Row length mismatch, skipping: {row}")
                    continue
                info.append({categories[j]: row[j].strip() for j in range(len(row))})
    return info



# Load data and process JSON structure
def load_data(data_folder: str):
    # Read files for mAbs KG
    mab_info = read_csv(os.path.join(data_folder, "Sample_MonoClonalAntibodies.csv"), ",")

    docs = {}

    # Create JSON for mAbs/virus/disease
    for row in mab_info:
        # Check required keys
        if not row.get('mab_id') or not row.get('virus_taxonid') or not row.get('virus_name'):
            print(f"Missing required fields in row: {row}")
            continue

        try:
            virus_taxonid = int(row['virus_taxonid'])  
        except ValueError:
            print(f"Invalid virus_taxonid value in row: {row}")
            continue

        doc_id = f"{row['mab_id']}-{virus_taxonid}"

        # Append to existing doc or create new doc
        if doc_id in docs:
            docs[doc_id]['relation'].append({
                'virus_taxonid': virus_taxonid,
                'virus_name': row['virus_name'],
            })
        else:
            docs[doc_id] = {
                '_id': doc_id,
                'subject': {
                    "type": "Antibody",
                    "name": row['mab_name'],
                    'id': row['mab_id'],
                },
                'object': {
                    "type": "Virus",
                    'id': virus_taxonid,
                    'name': row['virus_name'],
                },
                'relation': [{
                    'virus_taxonid': virus_taxonid,
                    'virus_name': row['virus_name'],
                }],
                'predicate': 'targets'
            }

    for doc in docs.values():
        yield doc


def test():
    obj = {'data': []}
    for doc in load_data("./"):  
        obj['data'].append(doc)

    with open("./output_mabs.json", "w") as f:
        json.dump(obj, f, indent=2)

    print("JSON file generated: output_mabs.json")


# Main execution
if __name__ == '__main__':
    test()

