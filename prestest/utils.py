from _pytest.fixtures import SubRequest


def get_prestest_params(request: SubRequest, param: str, default):
    """get value of target `param` in prestest fixtures. This search for the closest 'prestest' mark and extract the
    value from request param. If the mark is not present or target param is not found. return default value.

    :param request: pytest request
    :param param: name of the param to search
    :param default: default value if param is not found
    :return: value of param, or default if not found
    """
    container_param = request.node.get_closest_marker("prestest")
    if container_param is None:
        return default

    value = container_param.args[0].get(param)
    return value
