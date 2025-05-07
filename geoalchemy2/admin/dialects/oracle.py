"""This module defines specific functions for Oracle dialect."""

from sqlalchemy import text
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.elements import ClauseList

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


def init_oracle_spatial(dbapi_conn, *args, oracle_spatial_options: dict = None):
    """Initialize internal Oracle Spatial tables.

    Args:
        dbapi_conn: The DBAPI connection.
    """
    if oracle_spatial_options is not None:
        # Could use special options to configure behaviour
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
                     f"WHERE SDO_OWNER='{table.schema}' AND SDO_TABLE_NAME='{table.name.upper()}' "
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


def _compile_ST_DWithin_Oracle(element, compiler, **kw):
    element.identifier = "SDO_WITHIN_DISTANCE"
    d = list(element.clauses)[-1].value
    clauses = ClauseList(*element.clauses.clauses[:-1])
    compiled = compiler.process(clauses, **kw)

    return "{}({}, 'DISTANCE = {}') = 'TRUE'".format(element.identifier, compiled, d)


def _compile_ST_CoveredBy_Oracle(element, compiler, **kw):
    element.identifier = "SDO_RELATE"
    compiled = compiler.process(element.clauses, **kw)

    return "{}({}, 'mask=inside+touch') = 'TRUE'".format(element.identifier, compiled)


def _compile_ST_Relate_Oracle(element, compiler, **kw):
    element.identifier = "SDO_RELATE"
    pattern = list(element.clauses)[-1].value
    clauses = ClauseList(*element.clauses.clauses[:-1])
    compiled = compiler.process(clauses, **kw)

    return "{}({}, 'mask={}') = 'TRUE'".format(element.identifier, compiled, pattern)


def _compile_ST_GeomFromText_Oracle(element, compiler, **kw):
    element.identifier = "SDO_GEOMETRY"
    compiled = compiler.process(element.clauses, **kw)
    srid = element.type.srid

    if srid > 0:
        return "{}({}, {})".format(element.identifier, compiled, srid)
    else:
        return "{}({})".format(element.identifier, compiled)


def _compile_GeomFromWKB_Oracle(element, compiler, **kw):
    # SDO_UTIL.FROM_WKTGEOMETRY and SDO_UTIL.FROM_WKBGEOMETRY don't have the same number of parameters between 19c and 21c versions.
    # SRID parameter could be used only with 19c version of the database.
    # So, construction of a full SDO_GEOMETRY with SDO_ELEM_INFO_ARRAY and SDO_ORDINATE_ARRAY is required
    element.identifier = "SDO_GEOMETRY"
    geom_data = list(element.clauses)[0].value
    if isinstance(geom_data, memoryview):
        from shapely import get_coordinates
        from shapely.wkb import loads
        geom = loads(geom_data.tobytes().hex(), True)
        coordinates_list = get_coordinates(geom).tolist()
        append_coordinates = ",".join(
            f'{coordinate[0]},{coordinate[1]}' for coordinate in coordinates_list
        )
        list(element.clauses)[0].value = append_coordinates
        geom_data = append_coordinates

    # Support only polygon
    polygon = 'MDSYS.SDO_ELEM_INFO_ARRAY(1,1003,1)'
    ordinate_array = f'MDSYS.SDO_ORDINATE_ARRAY({geom_data})'

    clauses = ClauseList(*element.clauses.clauses[1:2])
    compiled_srid = compiler.process(clauses, **kw)  # Only the SRID
    compiled = f'2001,{compiled_srid},NULL,{polygon},{ordinate_array}'
    return "{}({})".format(element.identifier, compiled)


@compiles(functions.ST_Within, "oracle")  # type: ignore
def _Oracle_ST_Within(element, compiler, **kw):
    return _compile_ST_Within_Oracle(element, compiler, **kw)


@compiles(functions.ST_DWithin, "oracle")  # type: ignore
def _Oracle_ST_DWithin(element, compiler, **kw):
    return _compile_ST_DWithin_Oracle(element, compiler, **kw)


@compiles(functions.ST_CoveredBy, "oracle")  # type: ignore
def _Oracle_ST_CoveredBy(element, compiler, **kw):
    return _compile_ST_CoveredBy_Oracle(element, compiler, **kw)


@compiles(functions.ST_Relate, "oracle")  # type: ignore
def _Oracle_ST_Relate(element, compiler, **kw):
    return _compile_ST_Relate_Oracle(element, compiler, **kw)


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
