from pathlib import Path

from fidem.asyncpg.migrations import MigrationSection, load_section


def test_load_section_reads_sql_files_in_order_of_names(tmp_path: Path) -> None:
    (tmp_path / "0002_second.sql").write_text("SELECT 2")
    (tmp_path / "0001_first.sql").write_text("SELECT 1")
    (tmp_path / "README.md").write_text("not a migration")

    section = load_section(name="test", directory=tmp_path)

    assert section == MigrationSection(name="test", migrations=("SELECT 1", "SELECT 2"))
