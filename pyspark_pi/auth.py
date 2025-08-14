import requests.auth

try:
    from requests_kerberos import HTTPKerberosAuth
    _KERBEROS_AVAILABLE = True
except ImportError:
    _KERBEROS_AVAILABLE = False

from pyspark_pi import errors, ds_options

class BearerAuth(requests.auth.AuthBase):
    def __init__(self, token: str) -> None:
        self.token = token

    def __call__(self, req: requests.PreparedRequest) -> requests.PreparedRequest:
        req.headers["authorization"] = "Bearer " + self.token
        return req

def create_auth(
    config: 'ds_options.PiDataSourceConfig'
) -> requests.auth.AuthBase | None:
    if config.auth_method == ds_options.AuthMethod.ANONYMOUS:
        return None
    elif config.auth_method == ds_options.AuthMethod.BASIC:
        if not config.username or not config.password:
            raise errors.PiDataSourceInternalError("create_auth was called with basic auth but username or password is missing.")
        return requests.auth.HTTPBasicAuth(config.username, config.password)
    elif config.auth_method == ds_options.AuthMethod.BEARER:
        if not config.token:
            raise errors.PiDataSourceInternalError("create_auth was called with bearer auth but token is missing.")
        return BearerAuth(config.token)
    elif config.auth_method == ds_options.AuthMethod.KERBEROS:
        if not _KERBEROS_AVAILABLE:
            raise errors.PiDataSourceConfigError("Kerberos authentication is not available. Please install with pip install pyspark-pi[kerberos].")
        return HTTPKerberosAuth()
    else:
        raise errors.PiDataSourceConfigError(f"Unsupported authentication method: {config.auth_method}")