from __future__ import annotations

from app.services.schema_parser import parse_schema


def test_schema_parser_accepts_type_alias_and_field_fk() -> None:
    payload = {
        "tables": [
            {
                "name": "patients",
                "fields": [{"name": "id", "type": "bigint"}, {"name": "date_of_birth", "type": "timestamp"}],
            },
            {
                "name": "claims",
                "fields": [
                    {"name": "id", "type": "integer"},
                    {"name": "patient_id", "type": "int", "foreign_key": "patients.id"},
                    {"name": "cost", "type": "decimal"},
                ],
            },
        ]
    }

    parsed = parse_schema(payload)
    patients = next(t for t in parsed.tables if t.name == "patients")
    claims = next(t for t in parsed.tables if t.name == "claims")

    assert any(f.name == "date_of_birth" and f.type == "datetime" for f in patients.fields)
    assert any(f.name == "cost" and f.type == "float" for f in claims.fields)
    assert any(fk.column == "patient_id" and fk.ref_table == "patients" for fk in claims.foreign_keys)
