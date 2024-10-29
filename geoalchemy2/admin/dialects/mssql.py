"""This module defines specific functions for MSSQL dialect.

https://learn.microsoft.com/en-us/sql/relational-databases/spatial/create-construct-and-query-geometry-instances?view=sql-server-ver16
"""
import logging

from sqlalchemy import text
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.elements import ClauseList

from geoalchemy2 import functions
from geoalchemy2.types import Geometry

logger = logging.getLogger(__name__)


def create_spatial_index(bind, table, col):
    """Create spatial index on the given column."""
    # TODO https://learn.microsoft.com/en-us/sql/relational-databases/spatial/create-modify-and-drop-spatial-indexes?view=sql-server-ver16
    pass


def reflect_geometry_column(inspector, table, column_info):
    """Reflect a column of type Geometry with MSSQL dialect.
    All records should have the same SRID.
    https://gis.stackexchange.com/questions/19902/how-i-get-srid-from-geometry-field
    """
    if not isinstance(column_info.get("type"), Geometry):
        return
    # Update the srid
    if column_info["type"].srid == -1:
        statement = f"SELECT TOP(1) {column_info['name']}.STSrid from {table.schema}.{table.name}"
        srid_res = inspector.bind.execute(text(statement)).fetchone()
        if srid_res:
            column_info["type"].srid = srid_res[0]


def before_create(table, bind, **kw):
    """Handle spatial indexes during the before_create event."""
    pass


def after_create(table, bind, **kw):
    """Handle spatial indexes during the after_create event."""
    pass


def before_drop(table, bind, **kw):
    """Handle spatial indexes during the before_drop event."""
    pass


def after_drop(table, bind, **kw):
    """Handle spatial indexes during the after_drop event."""
    pass


def _fix_GIS_schema_issue(clause: str):
    # table schema are forbidden when using GIS functions 🥲
    chunks = clause.split('.')
    if len(chunks) > 2:
        schema = chunks.pop(0)
        logger.debug('dropping clause schema: %s', schema)
    return '.'.join(chunks)


def STAsBinary(element, compiler, **kw):
    clauses = compiler.process(element.clauses, **kw)
    clauses = _fix_GIS_schema_issue(clauses)
    return f"{clauses}.STAsBinary()"


def STGeomFromText(element, compiler, **kw):
    element.identifier = "geometry::STGeomFromText"
    compiled = compiler.process(element.clauses, **kw)
    srid = element.type.srid if element.type.srid > 0 else 0

    return "{}({}, {})".format(element.identifier, compiled, srid)


def STGeomFromWKB(element, compiler, **kw):
    element.identifier = "geometry::STGeomFromWKB"
    wkb_data = list(element.clauses)[0].value
    if isinstance(wkb_data, memoryview):
        list(element.clauses)[0].value = wkb_data.tobytes()
    compiled = compiler.process(element.clauses, **kw)
    srid = element.type.srid

    if srid > 0:
        return f"{element.identifier}({compiled}, {srid})"
    else:
        return f"{element.identifier}({compiled})"


def STWithin(element, compiler, **kw):
    obj = element.clauses.clauses[0]
    compiled_obj = compiler.process(obj, **kw)
    compiled_obj = _fix_GIS_schema_issue(compiled_obj)

    clauses = ClauseList(*element.clauses.clauses[1:])
    compiled_clauses = compiler.process(clauses, **kw)
    return f"{compiled_obj}.STWithin({compiled_clauses})=1"

def STDWithin(element, compiler, **kw):
    obj = element.clauses.clauses[0]
    compiled_obj = compiler.process(obj, **kw)
    compiled_obj = _fix_GIS_schema_issue(compiled_obj)

    d = list(element.clauses)[-1].value
    clauses = ClauseList(*element.clauses.clauses[1:-1])
    compiled_clauses = compiler.process(clauses, **kw)
    return f"{compiled_obj}.STDistance({compiled_clauses}) <= {d}"

def STCoveredBy(element, compiler, **kw):
    obj = element.clauses.clauses[0]
    compiled_obj = compiler.process(obj, **kw)
    compiled_obj = _fix_GIS_schema_issue(compiled_obj)

    clauses = ClauseList(*element.clauses.clauses[1:])
    compiled_clauses = compiler.process(clauses, **kw)
    return f"{compiled_obj}.STDistance({compiled_clauses}) <= 0"

def STRelate(element, compiler, **kw):
    obj = element.clauses.clauses[0]
    compiled_obj = compiler.process(obj, **kw)
    compiled_obj = _fix_GIS_schema_issue(compiled_obj)

    pattern = list(element.clauses)[-1].value
    clauses = ClauseList(*element.clauses.clauses[1:-1])
    compiled_clauses = compiler.process(clauses, **kw)
    return f"{compiled_obj}.STRelate({compiled_clauses}, '{pattern}') = 1"

compiles(functions.ST_AsBinary, "mssql")(STAsBinary)
compiles(functions.ST_AsWKB, "mssql")(STAsBinary)
compiles(functions.ST_AsEWKB, "mssql")(STAsBinary)

compiles(functions.ST_GeomFromWKB, "mssql")(STGeomFromWKB)
compiles(functions.ST_GeomFromEWKB, "mssql")(STGeomFromWKB)

compiles(functions.ST_GeomFromEWKT, "mssql")(STGeomFromText)

compiles(functions.ST_Within, "mssql")(STWithin)
compiles(functions.ST_DWithin, "mssql")(STDWithin)
compiles(functions.ST_CoveredBy, "mssql")(STCoveredBy)
compiles(functions.ST_Relate, "mssql")(STRelate)
