import requests
from requests_kerberos import HTTPKerberosAuth

from pyspark_pi import config, errors

class BearerAuth(requests.auth.AuthBase):
    def __init__(self, token):
        self.token = token
    def __call__(self, req):
        req.headers["authorization"] = "Bearer " + self.token
        return req

def create_auth(
        auth_method: config.AuthMethod,
        username: str = None,
        password: str = None,
        token: str = None
    ) -> requests.Session | None:
    """
    Create a requests session with the appropriate authentication method.
    """
    if auth_method == config.AuthMethod.ANONYMOUS:
        return None
    elif auth_method == config.AuthMethod.BASIC:
        return (username, password)
    elif auth_method == config.AuthMethod.BEARER:
        return BearerAuth(token)
    elif auth_method == config.AuthMethod.KERBEROS:
        return HTTPKerberosAuth()
    else:
        raise errors.PiDataSourceConfigError(f"Unsupported authentication method: {auth_method}")