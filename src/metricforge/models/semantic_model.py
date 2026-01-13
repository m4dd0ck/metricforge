"""Pydantic models for semantic model definitions."""

from enum import Enum

from pydantic import BaseModel, Field


class AggregationType(str, Enum):
    """Supported aggregation types for measures."""

    SUM = "sum"
    COUNT = "count"
    COUNT_DISTINCT = "count_distinct"
    AVG = "avg"
    MIN = "min"
    MAX = "max"


class DimensionType(str, Enum):
    """Types of dimensions."""

    CATEGORICAL = "categorical"
    TIME = "time"


class TimeGranularity(str, Enum):
    """Supported time granularities."""

    DAY = "day"
    WEEK = "week"
    MONTH = "month"
    QUARTER = "quarter"
    YEAR = "year"


class EntityType(str, Enum):
    """Types of entities in a semantic model."""

    PRIMARY = "primary"  # uniquely identifies a row
    FOREIGN = "foreign"  # references another model's primary
    UNIQUE = "unique"


class Entity(BaseModel):
    """An entity (key) in a semantic model."""

    name: str
    type: EntityType
    expr: str | None = None  # sql expression, defaults to name


class Measure(BaseModel):
    """A measure (aggregatable value) in a semantic model."""

    name: str
    agg: AggregationType
    expr: str
    description: str | None = None


class Dimension(BaseModel):
    """A dimension (groupable attribute) in a semantic model."""

    name: str
    type: DimensionType
    expr: str | None = None  # sql expression, defaults to name
    time_granularity: TimeGranularity | None = None
    description: str | None = None


class SemanticModel(BaseModel):
    """A semantic model maps to a single table/view."""

    name: str
    description: str | None = None
    table: str  # table name or ref('model_name') syntax from dbt
    primary_entity: str  # name of the primary entity - must exist in entities list
    entities: list[Entity] = Field(default_factory=list)
    measures: list[Measure] = Field(default_factory=list)
    dimensions: list[Dimension] = Field(default_factory=list)

    def get_measure(self, name: str) -> Measure | None:
        for measure in self.measures:
            if measure.name == name:
                return measure
        return None

    def get_dimension(self, name: str) -> Dimension | None:
        for dimension in self.dimensions:
            if dimension.name == name:
                return dimension
        return None

    def get_table_name(self) -> str:
        """Extract table name, handling dbt ref() syntax."""
        if self.table.startswith("ref("):
            # extract name from ref('name') or ref("name")
            start = self.table.find("'") + 1
            if start == 0:
                start = self.table.find('"') + 1
            end = self.table.rfind("'")
            if end == -1:
                end = self.table.rfind('"')
            return self.table[start:end]
        return self.table
