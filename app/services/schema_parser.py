from __future__ import annotations

from typing import Any

from app.models.schema import FieldSpec, ForeignKeySpec, ParsedSchema, TableSpec


class SchemaParserError(ValueError):
    pass


def parse_schema(schema_json: dict[str, Any]) -> ParsedSchema:
    """
    Supports:
    1) {"table_name":"t", "fields":[...]}
    2) {"tables":[{"name":"t1","fields":[...]}]}
    """
    if "tables" in schema_json:
        raw_tables = schema_json["tables"]
        if not isinstance(raw_tables, list) or not raw_tables:
            raise SchemaParserError("'tables' must be a non-empty list")
    else:
        raw_tables = [_single_table_to_multi(schema_json)]

    tables: list[TableSpec] = []
    for raw_table in raw_tables:
        table_name = raw_table.get("name")
        if not table_name:
            raise SchemaParserError("Each table must include a non-empty 'name'")

        fields = _normalize_fields(raw_table.get("fields", []))
        if not fields:
            raise SchemaParserError(f"Table '{table_name}' must include at least one field")

        primary_key = raw_table.get("primary_key", "id")
        if primary_key not in {f.name for f in fields}:
            fields = [FieldSpec(name=primary_key, type="integer", nullable=False)] + fields

        foreign_keys = [
            ForeignKeySpec(
                column=fk["column"],
                ref_table=fk["ref_table"],
                ref_column=fk.get("ref_column", "id"),
            )
            for fk in raw_table.get("foreign_keys", [])
        ]
        _append_field_level_foreign_keys(fields=fields, foreign_keys=foreign_keys)

        tables.append(
            TableSpec(
                name=table_name,
                primary_key=primary_key,
                fields=fields,
                foreign_keys=foreign_keys,
                row_multiplier=raw_table.get("row_multiplier", 1.0),
            )
        )

    tables = _infer_relationships(tables)
    relationships = {t.name: t.foreign_keys for t in tables}
    return ParsedSchema(tables=tables, relationships=relationships)


def _single_table_to_multi(schema_json: dict[str, Any]) -> dict[str, Any]:
    if "table_name" not in schema_json:
        raise SchemaParserError("Schema must contain either 'tables' or a single-table 'table_name'")
    return {
        "name": schema_json["table_name"],
        "primary_key": schema_json.get("primary_key", "id"),
        "fields": schema_json.get("fields", []),
        "foreign_keys": schema_json.get("foreign_keys", []),
    }


def _normalize_fields(raw_fields: list[Any]) -> list[FieldSpec]:
    fields: list[FieldSpec] = []
    for raw in raw_fields:
        if isinstance(raw, str):
            fields.append(FieldSpec(name=raw, type="string"))
            continue
        if isinstance(raw, dict):
            fields.append(FieldSpec(**raw))
            continue
        raise SchemaParserError("Field entries must be strings or objects")
    return fields


def _infer_relationships(tables: list[TableSpec]) -> list[TableSpec]:
    table_map = {table.name: table for table in tables}
    for table in tables:
        existing_fk_cols = {fk.column for fk in table.foreign_keys}
        for field in table.fields:
            if not field.name.endswith("_id"):
                continue
            if field.name in existing_fk_cols:
                continue
            candidate_table = field.name[: -len("_id")]
            if candidate_table in table_map and candidate_table != table.name:
                table.foreign_keys.append(
                    ForeignKeySpec(column=field.name, ref_table=candidate_table, ref_column="id")
                )
    return tables


def _append_field_level_foreign_keys(
    fields: list[FieldSpec],
    foreign_keys: list[ForeignKeySpec],
) -> None:
    existing = {(fk.column, fk.ref_table, fk.ref_column) for fk in foreign_keys}
    for field in fields:
        if not field.foreign_key:
            continue
        parsed = _parse_fk_reference(field.foreign_key)
        if parsed is None:
            continue
        ref_table, ref_column = parsed
        candidate = (field.name, ref_table, ref_column)
        if candidate in existing:
            continue
        foreign_keys.append(
            ForeignKeySpec(column=field.name, ref_table=ref_table, ref_column=ref_column)
        )
        existing.add(candidate)


def _parse_fk_reference(reference: str) -> tuple[str, str] | None:
    raw = reference.strip()
    if "." not in raw:
        return None
    left, right = raw.split(".", 1)
    table = left.strip()
    column = right.strip()
    if not table or not column:
        return None
    return table, column
