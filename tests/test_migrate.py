from database.migrate import checksum_of, migration_files


def test_checksum_ignores_line_endings():
    assert checksum_of("SELECT 1;\r\nSELECT 2;\r\n") == checksum_of("SELECT 1;\nSELECT 2;\n")
    assert checksum_of("SELECT 1;") != checksum_of("SELECT 2;")


def test_migrations_are_numbered_uniquely_and_in_order():
    names = [path.stem for path in migration_files()]
    numbers = [name[:3] for name in names]

    assert names, "no migrations found"
    assert len(numbers) == len(set(numbers)), "duplicate migration number"
    assert numbers == sorted(numbers)
