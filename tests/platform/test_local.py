from pathlib import Path

import pytest

from pipetree.platform.base import Platform
from pipetree.platform.local import LocalPlatform


def test_satisfies_the_platform_protocol():
    assert isinstance(LocalPlatform(), Platform)


def test_name_is_local():
    assert LocalPlatform().name == "local"


def test_resolve_secret_reads_an_environment_variable(monkeypatch):
    monkeypatch.setenv("ERP_LANDING_PATH", "/mnt/erp")

    assert LocalPlatform().resolve_secret("erp-landing-path") == "/mnt/erp"


def test_resolve_secret_normalizes_dashes_and_case(monkeypatch):
    monkeypatch.setenv("CRM_LANDING_PATH", "/mnt/crm")

    assert LocalPlatform().resolve_secret("crm-landing-path") == "/mnt/crm"


def test_resolve_secret_raises_a_clear_error_when_unset(monkeypatch):
    monkeypatch.delenv("MISSING_SECRET", raising=False)

    with pytest.raises(KeyError, match="MISSING_SECRET"):
        LocalPlatform().resolve_secret("missing-secret")


def test_qualify_table_name_is_a_no_op():
    assert LocalPlatform().qualify_table_name("bronze.orders") == "bronze.orders"


def test_resolve_path_joins_base_dir():
    result = LocalPlatform().resolve_path("data/customer.csv", Path("/tmp/example"))

    assert result == str(Path("/tmp/example/data/customer.csv"))


def test_run_metadata_is_empty():
    assert LocalPlatform().run_metadata() == {}
