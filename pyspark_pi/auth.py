import requests.auth

try:
    from requests_kerberos import HTTPKerberosAuth
    _KERBEROS_AVAILABLE = True
except ImportError:
    _KERBEROS_AVAILABLE = False

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
    ) -> requests.auth.AuthBase | None:
    """
    Create a requests session with the appropriate authentication method.
    """
    if auth_method == config.AuthMethod.ANONYMOUS:
        return None
    elif auth_method == config.AuthMethod.BASIC:
        return requests.auth.HTTPBasicAuth(username, password)
    elif auth_method == config.AuthMethod.BEARER:
        return BearerAuth(token)
    elif auth_method == config.AuthMethod.KERBEROS:
        if not _KERBEROS_AVAILABLE:
            raise errors.PiDataSourceConfigError("Kerberos authentication is not available. Please install with pip install pyspark-pi[kerberos].")
        return HTTPKerberosAuth()
    else:
        raise errors.PiDataSourceConfigError(f"Unsupported authentication method: {auth_method}")