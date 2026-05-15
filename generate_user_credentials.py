#!/usr/bin/env python
# Copyright 2018 Google LLC

from typing import List, Dict
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""This example will create an OAuth2 refresh token for the Google Ads API.

This example works with both web and desktop app OAuth client ID types.

https://console.cloud.google.com

IMPORTANT: For web app clients types, you must add "http://127.0.0.1:8080" to the
"Authorized redirect URIs" list in your Google Cloud Console project before
running this example. Desktop app client types do not require the local
redirect to be explicitly configured in the console.

Once complete, download the credentials and save the file path so it can be
passed into this example.

This example is a very simple implementation, for a more detailed example see:
https://developers.google.com/identity/protocols/oauth2/web-server#python
"""

import argparse
import hashlib
import os
import re
import webbrowser
import socket
import sys
from urllib.parse import parse_qs, unquote

# If using Web flow, the redirect URL must match exactly what’s configured in GCP for
# the OAuth client.  If using Desktop flow, the redirect must be a localhost URL and
# is not explicitly set in GCP.
from google_auth_oauthlib.flow import Flow

_SCOPE = "https://www.googleapis.com/auth/adwords"
_SERVER = "127.0.0.1"
_PORT = 8080
_REDIRECT_URI = f"http://{_SERVER}:{_PORT}"


def main(client_secrets_path: str, scopes: List[str]) -> None:
    """The main method, starts a basic server and initializes an auth request.

    Args:
        client_secrets_path: a path to where the client secrets JSON file is
          located on the machine running this example.
        scopes: a list of API scopes to include in the auth request, see:
            https://developers.google.com/identity/protocols/oauth2/scopes
    """
    flow = Flow.from_client_secrets_file(client_secrets_path, scopes=scopes)
    flow.redirect_uri = _REDIRECT_URI

    # Create an anti-forgery state token as described here:
    # https://developers.google.com/identity/protocols/OpenIDConnect#createxsrftoken
    passthrough_val = hashlib.sha256(os.urandom(1024)).hexdigest()

    authorization_url, expected_state = flow.authorization_url(
        access_type="offline",
        state=passthrough_val,
        prompt="consent",
        include_granted_scopes="true",
    )

    # Open default browser when possible; always print URL for copy-paste.
    print("Opening your browser for Google sign-in (or paste the URL below):\n")
    print(authorization_url)
    try:
        opened = webbrowser.open(authorization_url)
        if not opened:
            print(
                "\nCould not launch a browser automatically — paste the URL above manually."
            )
    except Exception as exc:
        print(f"\nCould not launch browser ({exc}) — paste the URL above manually.")
    print(f"\nWaiting for authorization and callback to: {_REDIRECT_URI}")

    # Retrieves an authorization code by opening a socket to receive the
    # redirect request and parsing the query parameters set in the URL.
    # Use state returned by the library (must match the ``state=`` in the auth URL).
    code = unquote(get_authorization_code(expected_state))

    # Pass the code back into the OAuth module to get a refresh token.
    flow.fetch_token(code=code)
    refresh_token = flow.credentials.refresh_token

    print(f"\nYour refresh token is: {refresh_token}\n")
    print(
        "Add your refresh token to your client library configuration as "
        "described here: "
        "https://developers.google.com/google-ads/api/docs/client-libs/python/configuration"
    )


def _recv_request_headers_prefix(connection: socket.socket, max_bytes: int = 262144) -> bytes:
    """Read until end of first line (at least ``\\r\\n``) so long ``code=`` values are not truncated."""
    buf = b""
    while len(buf) < max_bytes:
        chunk = connection.recv(8192)
        if not chunk:
            break
        buf += chunk
        if b"\r\n" in buf:
            break
    return buf


def get_authorization_code(expected_state: str) -> str:
    """Opens a socket to handle HTTP requests until the OAuth redirect arrives.

    Browsers often hit ``/favicon.ico`` (or other probes) before Google's
    redirect to ``/?code=...``; those requests are ignored so parsing does not
    fail with ``AttributeError``.

    Args:
        expected_state: CSRF ``state`` returned by ``flow.authorization_url`` (must
            match the ``state`` query param on the redirect).

    Returns:
        Authorization ``code`` query string from the Google Auth service.
    """
    sock = socket.socket()
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind((_SERVER, _PORT))
    sock.listen(8)
    message = "Authorization code was successfully retrieved."
    params: Dict[str, str] = {}
    try:
        while True:
            connection, address = sock.accept()
            data = _recv_request_headers_prefix(connection)
            params = parse_raw_query_params(data)
            if params.get("code") or params.get("error"):
                break
            # Not the OAuth callback (e.g. favicon) — close and wait for redirect.
            try:
                connection.sendall(
                    b"HTTP/1.1 204 No Content\r\nConnection: close\r\n\r\n"
                )
            except OSError:
                pass
            connection.close()

        try:
            if not params.get("code"):
                error = params.get("error")
                message = f"Failed to retrieve authorization code. Error: {error}"
                raise ValueError(message)
            if params.get("state") != expected_state:
                message = (
                    "State token does not match. Close every Google sign-in tab, "
                    "run this script again, and only complete login using the new URL "
                    "(do not reuse an old authorization link)."
                )
                raise ValueError(message)
            message = "Authorization code was successfully retrieved."
        except ValueError as error:
            print(error)
            response = (
                "HTTP/1.1 400 Bad Request\r\n"
                "Content-Type: text/html; charset=utf-8\r\n"
                "Connection: close\r\n\r\n"
                f"<b>{message}</b>"
                "<p>Please check the console output.</p>\n"
            )
            connection.sendall(response.encode())
            connection.close()
            sys.exit(1)

        response = (
            "HTTP/1.1 200 OK\r\n"
            "Content-Type: text/html; charset=utf-8\r\n"
            "Connection: close\r\n\r\n"
            f"<b>{message}</b>"
            "<p>Please check the console output.</p>\n"
        )
        connection.sendall(response.encode())
        connection.close()
    finally:
        sock.close()

    return params["code"]


def parse_raw_query_params(data: bytes) -> Dict[str, str]:
    """Parses the first line of a raw HTTP GET to extract ``/?...`` query params.

    Returns an empty dict if this is not a GET with a query string (e.g.
    ``GET /favicon.ico``).

    Args:
        data: raw request data as bytes.

    Returns:
        a dict of query parameter key value pairs.
    """
    decoded = data.decode("utf-8", errors="replace")
    first_line = decoded.split("\r\n", 1)[0]
    match = re.match(r"GET\s+/\?([^\s]+)\s+HTTP/", first_line)
    if not match:
        return {}
    qs = match.group(1)
    parsed = parse_qs(qs, keep_blank_values=True)
    return {k: (v[0] if v else "") for k, v in parsed.items()}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=(
            "Generates OAuth2 refresh token using the Web application flow. "
            "To retrieve the necessary client_secrets JSON file, first "
            "generate OAuth 2.0 credentials of type Web application in the "
            "Google Cloud Console (https://console.cloud.google.com). "
            "Make sure 'http://_SERVER:_PORT' is included the list of "
            "'Authorized redirect URIs' for this client ID."
        ),
    )
    # The following argument(s) should be provided to run the example.
    parser.add_argument(
        "-c",
        "--client_secrets_path",
        required=True,
        type=str,
        help=(
            "Path to the client secrets JSON file from the Google Developers "
            "Console that contains your client ID, client secret, and "
            "redirect URIs."
        ),
    )
    parser.add_argument(
        "--additional_scopes",
        default=None,
        type=str,
        nargs="+",
        help="Additional scopes to apply when generating the refresh token.",
    )
    args = parser.parse_args()

    configured_scopes = [_SCOPE]

    if args.additional_scopes:
        configured_scopes.extend(args.additional_scopes)

    main(args.client_secrets_path, configured_scopes)
