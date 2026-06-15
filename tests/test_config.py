from pipeline import config


def test_canonical_columns_order():
    assert config.CANONICAL_SETTLEMENT_COLUMNS == [
        "transaction_id", "merchant_id", "settled_amount", "currency",
        "settled_at", "processor_reference", "status", "source_name",
    ]


def test_paysettler_source_registered():
    src = config.SOURCES["paysettler"]
    assert src.decimal_separator == ","
    assert src.file_glob.endswith("settlement_paysettler.csv")
    assert src.assume_utc is True
