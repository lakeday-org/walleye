"""GitHub repository inputs, installation authentication, and a small REST client."""

import json
import os
import re
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlparse
from urllib.request import Request, urlopen

import jwt


@dataclass(frozen=True)
class Repository:
    owner: str
    name: str
    issue: int | None = None

    @property
    def full_name(self):
        return f"{self.owner}/{self.name}"

    @property
    def url(self):
        return f"https://github.com/{self.full_name}"


def repository_input(value):
    value = str(value)
    if Path(value).exists():
        return None
    if value.startswith("https://github.com/"):
        url = urlparse(value)
        if url.query or url.fragment or url.username or url.password:
            raise ValueError("Use a GitHub repository URL or issue URL without query parameters")
        value = url.path.strip("/")
    elif "://" in value or value.startswith(("/", ".")):
        return None
    match = re.fullmatch(
        r"([A-Za-z0-9][A-Za-z0-9-]*)/([A-Za-z0-9_.-]+?)(?:\.git)?(?:/issues/([1-9][0-9]*))?", value
    )
    if not match or match[2] in {".", ".."}:
        return None
    return Repository(match[1], match[2], int(match[3]) if match[3] else None)


def http_request(method, path, token, payload=None):
    if not path.startswith("/") or path.startswith("//"):
        raise ValueError("Invalid GitHub API path")
    headers = {"Accept": "application/vnd.github+json", "X-GitHub-Api-Version": "2026-03-10"}
    if token:
        headers["Authorization"] = "Bearer " + token
    data = None if payload is None else json.dumps(payload).encode()
    if data is not None:
        headers["Content-Type"] = "application/json"
    request = Request("https://api.github.com" + path, data=data, headers=headers, method=method)
    try:
        with urlopen(request, timeout=60) as response:
            return json.load(response)
    except HTTPError as error:
        # Response bodies and request headers can contain credentials or user content.
        raise ValueError(
            f"GitHub {method} {path.split('?')[0]} failed: HTTP {error.code}"
        ) from None
    except (URLError, TimeoutError, json.JSONDecodeError) as error:
        raise ValueError(
            f"GitHub {method} failed ({type(error).__name__}); "
            "inspect saved publication state before retrying"
        ) from None


class GitHub:
    def __init__(self, repository, *, request=http_request, environ=None, clock=time.time):
        self.repository, self.request, self.clock = repository, request, clock
        self.env = dict(os.environ if environ is None else environ)
        self._token = self.env.get("GITHUB_TOKEN") or self.env.get("GH_TOKEN")
        self.app_id = self.env.get("GITHUB_APP_ID") or self.env.get("GITHUB_APP_CLIENT_ID")
        self.key_path = self.env.get("GITHUB_APP_PRIVATE_KEY_PATH")
        self.installation = self.env.get("GITHUB_APP_INSTALLATION_ID")
        self.expires = 0
        self._identity = None
        if bool(self.app_id) != bool(self.key_path):
            raise ValueError("Set both GITHUB_APP_ID and GITHUB_APP_PRIVATE_KEY_PATH")
        self.auth_kind = "app" if self.app_id else "token" if self._token else "anonymous"
        if not self.app_id and not self._token and self.env.get("WALLEYE_GITHUB_AUTH") == "gh":
            result = subprocess.run(
                ["gh", "auth", "token", "--hostname", "github.com"],
                capture_output=True,
                text=True,
                timeout=30,
            )
            if result.returncode or not result.stdout.strip():
                raise ValueError("The requested gh login is unavailable")
            self._token, self.auth_kind = result.stdout.strip(), "gh-user"

    def app_jwt(self):
        now = int(self.clock())
        try:
            return jwt.encode(
                {"iat": now - 60, "exp": now + 540, "iss": self.app_id},
                Path(self.key_path).read_bytes(),
                algorithm="RS256",
            )
        except (OSError, ValueError, jwt.PyJWTError):
            raise ValueError("Cannot read or sign with the GitHub App private key") from None

    def token(self, *, required=False):
        if self.app_id and self.clock() >= self.expires - 60:
            signed = self.app_jwt()
            if not self.installation:
                data = self.request(
                    "GET", f"/repos/{self.repository.full_name}/installation", signed
                )
                self.installation = str(data["id"])
            if not str(self.installation).isdigit():
                raise ValueError("GitHub App installation ID must be numeric")
            data = self.request(
                "POST",
                f"/app/installations/{self.installation}/access_tokens",
                signed,
                {"repositories": [self.repository.name]},
            )
            self._token = data["token"]
            from datetime import datetime

            self.expires = datetime.fromisoformat(
                data["expires_at"].replace("Z", "+00:00")
            ).timestamp()
        if required and not self._token:
            raise ValueError(
                "GitHub publishing needs App credentials or GITHUB_TOKEN; "
                "use WALLEYE_GITHUB_AUTH=gh for an explicit local development fallback"
            )
        return self._token

    def commit_identity(self):
        if self._identity is None:
            token = self.token(required=True)
            if self.app_id:
                app = self.request("GET", "/app", self.app_jwt())
                path = "/users/" + quote(app["slug"] + "[bot]", safe="")
            else:
                path = "/user"
            try:
                account = self.request("GET", path, token)
            except ValueError as error:
                raise ValueError(
                    "Cannot resolve the authenticated commit author. "
                    "Installation tokens need the App ID and private key credentials."
                ) from error
            self._identity = {
                "name": account.get("name") or account["login"],
                "email": f"{account['id']}+{account['login']}@users.noreply.github.com",
            }
        return self._identity

    def api(self, method, suffix="", payload=None):
        return self.request(
            method,
            f"/repos/{self.repository.full_name}{suffix}",
            self.token(required=method != "GET"),
            payload,
        )

    def pages(self, suffix):
        separator = "&" if "?" in suffix else "?"
        page = 1
        while True:
            rows = self.api("GET", f"{suffix}{separator}per_page=100&page={page}")
            yield from rows
            if len(rows) < 100:
                return
            page += 1
