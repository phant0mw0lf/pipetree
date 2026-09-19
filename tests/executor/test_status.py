from pipetree.executor.status import RunDigest, TableResult, TableStatus


def make_result(fqn: str, status: TableStatus) -> TableResult:
    return TableResult(
        table_fqn=fqn,
        status=status,
        attempts=1,
        started_at=250908143012456,
        ended_at=250908143012999,
        duration_ms=543,
    )


def test_run_digest_succeeded_when_every_table_succeeded_or_recovered():
    digest = RunDigest(
        execution_id=250908143012456,
        results={
            "bronze.orders": make_result("bronze.orders", TableStatus.SUCCEEDED),
            "silver.orders": make_result("silver.orders", TableStatus.RETRIED_SUCCEEDED),
        },
    )

    assert digest.succeeded is True
    assert digest.exit_code == 0


def test_run_digest_fails_when_any_table_failed():
    digest = RunDigest(
        execution_id=250908143012456,
        results={
            "bronze.orders": make_result("bronze.orders", TableStatus.SUCCEEDED),
            "silver.orders": make_result("silver.orders", TableStatus.FAILED),
        },
    )

    assert digest.succeeded is False
    assert digest.exit_code != 0


def test_run_digest_fails_when_any_table_is_upstream_failed():
    digest = RunDigest(
        execution_id=250908143012456,
        results={
            "gold.dim_customer": make_result("gold.dim_customer", TableStatus.UPSTREAM_FAILED),
        },
    )

    assert digest.succeeded is False


def test_table_status_string_values_match_the_post_vocabulary():
    assert TableStatus.SUCCEEDED.value == "succeeded"
    assert TableStatus.FAILED.value == "failed"
    assert TableStatus.RETRIED_SUCCEEDED.value == "retried→succeeded"
    assert TableStatus.UPSTREAM_FAILED.value == "upstream_failed"
    assert TableStatus.SKIPPED.value == "skipped"
