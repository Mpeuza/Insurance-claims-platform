from src.insurance_claims.config_loader import SourceConfig


def test_source_config_creation():
    cfg = SourceConfig(
        source_name="claims", source_type="file", source_path="/mnt/raw/claims/",
        target_catalog="test", target_schema="bronze_insurance_claims", target_table="claims",
        load_type="full", watermark_column=None, secret_scope="keyvault",
        secret_key="claims-source-key", is_active=True, max_records_per_file=1000000, retries=3
    )
    assert cfg.source_name == "claims"
    assert cfg.is_active is True