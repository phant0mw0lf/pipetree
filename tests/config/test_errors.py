from pipetree.config.errors import ConfigError


def test_config_error_message_includes_key_path_and_reason():
    err = ConfigError(
        path="silver.tables.orders.merge.delete_mode",
        reason="'purge' is not one of soft|hard|ignore",
    )

    assert str(err) == (
        "silver.tables.orders.merge.delete_mode: 'purge' is not one of soft|hard|ignore"
    )
    assert err.path == "silver.tables.orders.merge.delete_mode"
