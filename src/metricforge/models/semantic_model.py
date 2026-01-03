"""Pydantic models for semantic model definitions.

semantic models define the "what" - tables, measures, dimensions, entities.
this is the core abstraction that makes metrics composable and reusable.
borrowed heavily from the metricflow/dbt semantic layer design patterns.
"""

from enum import Enum

from pydantic import BaseModel, Field


class AggregationType(str, Enum):
    """Supported aggregation types for measures.

    keeping this to standard sql aggregates for now. percentile/median would
    be nice but they're not as portable across databases.
    """

    SUM = "sum"
    COUNT = "count"
    COUNT_DISTINCT = "count_distinct"  # common enough to warrant its own type
    AVG = "avg"
    MIN = "min"
    MAX = "max"
    # TODO: might want to add MEDIAN, PERCENTILE someday - duckdb supports them


class DimensionType(str, Enum):
    """Types of dimensions.

    time dimensions get special treatment for date filtering and grain truncation.
    everything else is categorical for now.
    """

    CATEGORICAL = "categorical"
    TIME = "time"


class TimeGranularity(str, Enum):
    """Supported time granularities.

    these map directly to duckdb's date_trunc function which is convenient.
    """

    DAY = "day"
    WEEK = "week"
    MONTH = "month"
    QUARTER = "quarter"
    YEAR = "year"


class EntityType(str, Enum):
    """Types of entities in a semantic model.

    entities define how models can be joined - primary keys, foreign keys, etc.
    this is the foundation for multi-table queries (not implemented yet).
    """

    PRIMARY = "primary"  # uniquely identifies a row
    FOREIGN = "foreign"  # references another model's primary
    UNIQUE = "unique"    # unique but not the primary key


class Entity(BaseModel):
    """An entity (key) in a semantic model.

    I originally just had primary_key as a string field on the model,
    but entities are more flexible for joins.
    """

    name: str
    type: EntityType
    expr: str | None = None  # sql expression, defaults to name


class Measure(BaseModel):
    """A measure (aggregatable value) in a semantic model.

    measures are the building blocks of metrics. expr can be a simple column
    name or a sql expression like "price * quantity". the aggregation type
    determines how values are combined.
    """

    name: str
    agg: AggregationType
    expr: str  # sql expression - can be column name or calculation like "price * qty"
    description: str | None = None


class Dimension(BaseModel):
    """A dimension (groupable attribute) in a semantic model.

    dimensions are what you group by in queries. time dimensions are special -
    they support automatic grain truncation (day -> week -> month).
    """

    name: str
    type: DimensionType
    expr: str | None = None  # sql expression, defaults to name
    time_granularity: TimeGranularity | None = None  # required for time type
    description: str | None = None
    # could probably add is_partition_key for query optimization hints


class SemanticModel(BaseModel):
    """A semantic model maps to a single table/view.

    this is where the magic happens - the semantic model provides the mapping
    between business concepts (measures, dimensions) and physical tables.
    one model = one table, keeps things simple.
    """

    name: str
    description: str | None = None
    table: str  # table name or ref('model_name') syntax from dbt
    primary_entity: str  # name of the primary entity - must exist in entities list
    entities: list[Entity] = Field(default_factory=list)
    measures: list[Measure] = Field(default_factory=list)
    dimensions: list[Dimension] = Field(default_factory=list)

    # helper methods for looking up measures/dimensions by name
    # could use a dict for O(1) lookup but lists are small in practice

    def get_measure(self, name: str) -> Measure | None:
        """Get a measure by name."""
        for measure in self.measures:
            if measure.name == name:
                return measure
        return None

    def get_dimension(self, name: str) -> Dimension | None:
        """Get a dimension by name."""
        for dimension in self.dimensions:
            if dimension.name == name:
                return dimension
        return None

    def get_table_name(self) -> str:
        """Extract the actual table name from the table reference.

        handles dbt-style ref('model_name') syntax since many folks will be
        coming from that ecosystem. bit hacky with the string parsing but
        didn't want to pull in a full parser just for this.
        """
        # handle ref('model_name') syntax
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
