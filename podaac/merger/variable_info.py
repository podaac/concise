"""Wrapper used to manage variable metadata"""


class VariableInfo:
    """
    Lightweight wrapper class utilized in granule preprocessing to simply comparisons between
    different variables from different granule sets

    Attributes
    ----------
    name: str
        name of the variable
    dim_order: list
        list of dimension names in order
    datatype: numpy.dtype
        the numpy datatype for the data held in the variable
    group_path: str
        Unix-like group path to the variable
    fill_value: object
        Value used to fill missing/empty values in variable's data
    """

    def __init__(self, var):
        self.name = var.name
        self.dim_order = var.dimensions
        self.datatype = var.datatype
        self.group_path = var.group().path

        if hasattr(var, '_FillValue'):
            self.fill_value = var._FillValue
        elif hasattr(var, 'missing_value'):
            self.fill_value = var.missing_value
        else:
            self.fill_value = None

        self.init = True  # Finalize object values

    def __setattr__(self, name, value):
        if hasattr(self, 'init') and self.init:
            raise AttributeError('VariableInfo is immutable')

        self.__dict__[name] = value

    def __eq__(self, other):
        return (
            self.dim_order == other.dim_order and
            self.datatype == other.datatype and
            self.name == other.name and
            self.fill_value == other.fill_value and
            self.group_path == other.group_path
        )