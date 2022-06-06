import environ
env = environ.Env()
environ.Env.read_env()

NEW_DRUGS2 = ['50242004062', '50242021401', '50242021486', '50242004086',
              '50242021586', '50242021501', '12064001801', '12064001800',
              '00173088161', '00173088101', '00310173085', '00310173030',
              '00024591401', '00024591402', '00024591801', '00024591601',
              '00024591802'
              ]
NEO4J_HOST = env('NEO4J_HOST')

NEO4J_USER = env('NEO4J_USER')

NEO4J_PASSWORD = env('NEO4J_PASSWORD')

MATCHED_DRUG_QUERY = """
MATCH (c:concepts)
WHERE c.vocabulary_id IN ["NDC", "HCPCS", "ICD10PCS", "ICD10" ,"ICD9Proc"]
  AND c.concept_code IN {}
RETURN c.concept_code as actual_ndc, c.concept_code as packagendc,  c.description as packagedescription , c.brand_name as brandname, c.molecule_name as genericname, c.drug_class as drugclass
"""

UNMATCHED_DRUG_QUERY = """
MATCH (c:concepts)
WHERE c.vocabulary_id IN ["NDC", "HCPCS", "ICD10PCS", "ICD10" ,"ICD9Proc"]
    AND c.concept_code IN {}
RETURN c.concept_code as concept_code
"""

ALL_DRUG_QUERY = """
MATCH (c:concepts)
WHERE c.vocabulary_id IN ["NDC", "HCPCS", "ICD10PCS", "ICD10" ,"ICD9Proc"]
RETURN c.concept_code as concept_code
"""