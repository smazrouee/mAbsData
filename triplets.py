'_id': doc_id,
                'subject': {
                    "type": "ChemicalMiture",
                    'id': row['mab_id'],
                    "name": row['mab_name'],
                },
                'object': {
                    "type": "Virus",
                    'id': int(row['virus_id']),
                    'name': row['virus_name'],
                },
                "pubmed": <pubmed_id>,
                'predicate': 'targets'
            }



'_id': doc_id,
                'subject': {
                    "type": "virus",
                    "name": row['virus_name'],
                    'id': row['virus_id'],
                },
                'object': {
                    "type": "disease",
                    "id": int(row['disease_id']),
                    'name': row['disease_name'],
                },
                'predicate': 'causes'
            }




'_id': doc_id,
                'subject': {
                    "type": "disease",
                    "id": row['disease_id'],
                    "name": row['disease_name'],
                },
                'object': {
                    "type": "phenotypic_feature",
                    'id': int(row['phetotypic_feature_id']),
                    'name': row['phenotypic_feature_name'],
                },
                'predicate': 'exhibits'
            }





'_id': doc_id,
                'subject': {
                    "type": "Antibody",
                    "id": row['mab_id'],
                    "name": row['mab_name'],
                },
                'object': {
                    "type": "phenotypic_feature",
                    "id": int(row['symptom_id']),
                    "name": row['symptom_name'],
                },
                'predicate': 'ameliorates'
            }




'_id': doc_id,
                'subject': {
                    "type": "ChemicalMixture",
                    'id': row['mab_id'],
                    "name": row['mab_name'],
                },
                'object': {
                    "type": "disease",
                    'id': int(row['disease_id']),
                    'name': row['disease_name'],
                },
                'predicate': 'treats'
            }
"mAb_triplets.py" 100L, 2689B
