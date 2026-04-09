from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field, field_validator


AllowedFieldType = Literal["string", "integer", "float", "boolean", "date", "datetime"]


class FieldSpec(BaseModel):
    name: str = Field(min_length=1)
    type: AllowedFieldType = "string"
    nullable: bool = True
    faker: str | None = None
    foreign_key: str | None = None
    pii: bool = False
    distribution: str | None = None
    min_value: float | None = None
    max_value: float | None = None
    allowed_values: list[Any] | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator("type", mode="before")
    @classmethod
    def normalize_type(cls, value: Any) -> AllowedFieldType:
        if value is None:
            return "string"
        normalized = str(value).strip().lower()

        alias_map: dict[str, AllowedFieldType] = {
            # string-like
            "string": "string",
            "str": "string",
            "text": "string",
            "varchar": "string",
            "char": "string",
            "uuid": "string",
            "json": "string",
            "object": "string",
            # integer-like
            "int": "integer",
            "integer": "integer",
            "long": "integer",
            "bigint": "integer",
            "smallint": "integer",
            "tinyint": "integer",
            # float-like
            "float": "float",
            "double": "float",
            "decimal": "float",
            "numeric": "float",
            "real": "float",
            # boolean-like
            "bool": "boolean",
            "boolean": "boolean",
            # temporal
            "date": "date",
            "datetime": "datetime",
            "timestamp": "datetime",
            "timestamptz": "datetime",
        }
        # Unknown types are safely treated as strings for versatility.
        return alias_map.get(normalized, "string")


class ForeignKeySpec(BaseModel):
    column: str
    ref_table: str
    ref_column: str = "id"


class TableSpec(BaseModel):
    name: str = Field(min_length=1)
    primary_key: str = "id"
    fields: list[FieldSpec]
    foreign_keys: list[ForeignKeySpec] = Field(default_factory=list)
    row_multiplier: float = Field(default=1.0, ge=0.1, le=100.0)


class ParsedSchema(BaseModel):
    tables: list[TableSpec]
    relationships: dict[str, list[ForeignKeySpec]] = Field(default_factory=dict)
