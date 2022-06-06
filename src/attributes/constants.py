ATTRIBUTES_LIST = ['Patient',
                   'Provider',
                   'Treatment',
                   'Derived'
                   ]

TREATMENT_CUSTOM = [
    {
        'id': 1,
        'name': 'Drug class 1',
        'description': '',
        'codes': [

        ],
        'keys': [

        ]
    }
]

SPECIALTY_QUERY = """
MATCH (n:concepts)
WHERE n.domain_id = 'Provider' AND n.standard_concept = 'S' AND n.concept_id in {}
RETURN n.concept_id as concept_id, n.concept_code as concept_code, n.description as description 
"""

GENDER_QUERY = """
MATCH (n:concepts)
WHERE n.vocabulary_id = 'Gender' AND n.concept_id in {}
RETURN n.concept_id as concept_id, n.concept_code as concept_code, n.description as description
"""

DRUG_CODE_OUTPUT_QUERY = """
MATCH (c:concepts)
WHERE c.vocabulary_id IN ["NDC", "HCPCS", "ICD10PCS", "ICD10" ,"ICD9Proc"]
  AND c.concept_code IN {}
RETURN c.concept_code as source_value, c.description as code_description, c.vocabulary_id as code_type, "" as default_dos_value
"""
