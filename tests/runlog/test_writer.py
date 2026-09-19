from pipetree.runlog.writer import InMemoryRunLogWriter, RunLogWriter


def test_in_memory_writer_satisfies_the_run_log_writer_protocol():
    assert isinstance(InMemoryRunLogWriter(), RunLogWriter)


def test_in_memory_writer_stores_rows_for_an_execution_id():
    writer = InMemoryRunLogWriter()
    rows = [{"table_fqn": "bronze.orders", "status": "succeeded"}]

    writer.write(rows, execution_id=1)

    assert writer.rows_for(1) == rows
    assert writer.write_calls == 1


def test_in_memory_writer_overwrite_is_idempotent_for_the_same_execution_id():
    writer = InMemoryRunLogWriter()
    writer.write([{"table_fqn": "bronze.orders", "status": "failed"}], execution_id=1)

    # a re-run of the same execution_id (e.g. after a crash) replaces, not appends
    writer.write([{"table_fqn": "bronze.orders", "status": "succeeded"}], execution_id=1)

    assert writer.rows_for(1) == [{"table_fqn": "bronze.orders", "status": "succeeded"}]
    assert writer.write_calls == 2


def test_in_memory_writer_keeps_different_execution_ids_separate():
    writer = InMemoryRunLogWriter()
    writer.write([{"table_fqn": "bronze.orders"}], execution_id=1)
    writer.write([{"table_fqn": "bronze.orders"}], execution_id=2)

    assert len(writer.rows_for(1)) == 1
    assert len(writer.rows_for(2)) == 1
    assert len(writer.rows) == 2
