"""This module defines specific functions for MSSQL dialect."""
from geoalchemy2.elements import RasterElement
from geoalchemy2.elements import WKBElement
from geoalchemy2.elements import WKTElement
from geoalchemy2.shape import to_shape


def bind_processor_process(spatial_type, bindvalue):
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
