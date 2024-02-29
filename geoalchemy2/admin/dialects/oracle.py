"""This module defines specific functions for Oracle dialect."""

from sqlalchemy import text
from sqlalchemy.ext.compiler import compiles

from geoalchemy2 import functions
from geoalchemy2.types import Geometry


def load_oracle_spatial_driver(dbapi_conn, *args):
    """Load Oracle Spatial extension in Oracle connection.

    .. Warning::
        Oracle Database Express editions are not supported because
        these databases don't include the Java virtual machine.
        The virtual machine is needed for the WKT conversion routines
        which are Java stored procedures.
        https://forums.oracle.com/ords/apexds/post/sdo-geometry-wkt-string-on-oracle-express-9945
        With SQLPLUS and an Express edition, this statement SELECT SDO_GEOMETRY('POINT(0 0)', 8307) FROM dual
        returns the following error:
            ERROR at line 1:
            ORA-29538: Java not installed
            ORA-06512: at "MDSYS.SDO_JAVA_STP", line 82
            ORA-06512: at "MDSYS.SDO_UTIL", line 7336
            ORA-06512: at "MDSYS.SDO_GEOMETRY", line 180
            ORA-06512: at line 1
    Args:
        dbapi_conn: The DBAPI connection.
    """
    version_cur = dbapi_conn.cursor()
    version_cur.execute("SELECT banner FROM v$version WHERE banner LIKE 'Oracle%'")
    if version_cur.fetchone()[0].find('Express') != -1:
        raise RuntimeError("The Express edition of the Oracle database is not supported.")


def init_oracle_spatial(dbapi_conn, *args):
    """Initialize internal Oracle Spatial tables.

    Args:
        dbapi_conn: The DBAPI connection.
    """
    pass


def load_oracle_spatial(*args, **kwargs):
    """Load Oracle Spatial extension and initialize internal tables.

    See :func:`geoalchemy2.admin.dialects.oracle.load_oracle_spatial_driver` and
    :func:`geoalchemy2.admin.dialects.oracle.init_oracle_spatial` functions for details about
    arguments.
    """
    load_oracle_spatial_driver(*args)
    init_oracle_spatial(*args, **kwargs)


def get_oracle_spatial_version(bind):
    """Get the version of the currently loaded extension."""
    return bind.execute(text("SELECT OPG_APIS.GET_VERSION() FROM DUAL;")).fetchone()[0]


def create_spatial_index(bind, table, col):
    """Create spatial index on the given column."""
    pass


def reflect_geometry_column(inspector, table, column_info):
    """Reflect a column of type Geometry with Oracle dialect."""
    if not isinstance(column_info.get("type"), Geometry):
        return
    # Update the srid
    if column_info["type"].srid == -1:
        statement = ("SELECT SDO_SRID FROM MDSYS.SDO_GEOM_METADATA_TABLE "
                     f"WHERE SDO_TABLE_NAME='{table.schema}.{table.name.upper()}' "
                     f"AND SDO_COLUMN_NAME='{column_info['name'].upper()}'")
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


# Define compiled versions for functions in Oracle.
_ORACLE_FUNCTIONS = {
    "ST_AsBinary": "SDO_UTIL.TO_WKBGEOMETRY",
    "ST_AsEWKB": "SDO_UTIL.TO_WKBGEOMETRY",
    "ST_AsGeoJSON": "SDO_UTIL.TO_GEOJSON",
}


def _compiles_oracle(cls, fn):
    def _compile_oracle(element, compiler, **kw):
        return "{}({})".format(fn, compiler.process(element.clauses, **kw))

    compiles(getattr(functions, cls), "oracle")(_compile_oracle)


def register_oracle_mapping(mapping):
    """Register compilation mappings for the given functions.

    Args:
        mapping: Should have the following form::

                {
                    "function_name_1": "oracle_function_name_1",
                    "function_name_2": "oracle_function_name_2",
                    ...
                }
    """
    for cls, fn in mapping.items():
        _compiles_oracle(cls, fn)


register_oracle_mapping(_ORACLE_FUNCTIONS)


def _compile_ST_Within_Oracle(element, compiler, **kw):
    element.identifier = "SDO_INSIDE"
    compiled = compiler.process(element.clauses, **kw)

    return "{}({}) = 'TRUE'".format(element.identifier, compiled)


def _compile_ST_GeomFromText_Oracle(element, compiler, **kw):
    element.identifier = "SDO_GEOMETRY"
    compiled = compiler.process(element.clauses, **kw)
    srid = element.type.srid

    if srid > 0:
        return "{}({}, {})".format(element.identifier, compiled, srid)
    else:
        return "{}({})".format(element.identifier, compiled)


def _compile_GeomFromWKB_Oracle(element, compiler, **kw):
    element.identifier = "SDO_GEOMETRY"
    wkb_data = list(element.clauses)[0].value
    if isinstance(wkb_data, memoryview):
        list(element.clauses)[0].value = wkb_data.tobytes().hex()
    compiled = compiler.process(element.clauses, **kw)

    # Use TO_BLOB to convert the hexadecimal string
    compiled_list = compiled.split(',')
    compiled_list[0] = f"TO_BLOB({compiled_list[0]})"
    compiled = ','.join(c for c in compiled_list)
    srid = element.type.srid

    if srid > 0:
        return "{}({}, {})".format(element.identifier, compiled, srid)
    else:
        return "{}({})".format(element.identifier, compiled)


@compiles(functions.ST_Within, "oracle")  # type: ignore
def _Oracle_ST_Within(element, compiler, **kw):
    return _compile_ST_Within_Oracle(element, compiler, **kw)


@compiles(functions.ST_GeomFromEWKB, "oracle")  # type: ignore
def _Oracle_ST_GeomFromEWKB(element, compiler, **kw):
    return _compile_GeomFromWKB_Oracle(element, compiler, **kw)


@compiles(functions.ST_GeomFromWKB, "oracle")  # type: ignore
def _Oracle_ST_GeomFromWKB(element, compiler, **kw):
    return _compile_GeomFromWKB_Oracle(element, compiler, **kw)


@compiles(functions.ST_GeomFromEWKT, "oracle")  # type: ignore
def _Oracle_ST_GeomFromEWKT(element, compiler, **kw):
    return _compile_ST_GeomFromText_Oracle(element, compiler, **kw)


@compiles(functions.ST_GeomFromText, "oracle")  # type: ignore
def _Oracle_ST_GeomFromText(element, compiler, **kw):
    return _compile_ST_GeomFromText_Oracle(element, compiler, **kw)
