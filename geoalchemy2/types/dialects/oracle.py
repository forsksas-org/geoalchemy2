"""This module defines specific functions for Oracle dialect."""
from geoalchemy2.elements import RasterElement
from geoalchemy2.elements import WKBElement
from geoalchemy2.elements import WKTElement
from geoalchemy2.shape import to_shape


def bind_processor_process(spatial_type, bindvalue):
    """
    This function is used to compile queries to SQL (not required to run them through an SQL driver).
    To ensure that geoalchemy queries are compilable to SQL, you must code it explicitely (see SQLGeometry)

    MSSQL geometry::STGeomFromText(wkt, srid) takes 2 parameters, however, with SQLAlchemy we can only generate one bind parameter.
    Thus, this function returns only WKT.
    Srid parameter is built inline by admin.dialects.mssql.STGeomFromText (not a bind param)
    Note: overriding coerce_compared_value method doesn't work, because geometry literals are often used by functions, which is not considered as a "comparison"
    """
    if isinstance(bindvalue, WKTElement):
        return f"{bindvalue.data}"
    elif isinstance(bindvalue, WKBElement):
        if not bindvalue.extended:
            shape = to_shape(bindvalue)
            return f"{shape.wkt}"
        else:
            return bindvalue.desc
    elif isinstance(bindvalue, RasterElement):
        return f"{bindvalue.data}"
    else:
        return bindvalue
