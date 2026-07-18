INSERT INTO ${catalog}.control.ingest_config VALUES
('claims', 'file', '/mnt/raw/claims/', '${catalog}', 'bronze_insurance_claims', 'claims',
 'full', NULL, 'keyvault', 'claims-source-key', true, 1000000, 3),
('policies', 'file', '/mnt/raw/policies/', '${catalog}', 'bronze_insurance_claims', 'policies',
 'full', NULL, 'keyvault', 'policies-source-key', true, 1000000, 3);