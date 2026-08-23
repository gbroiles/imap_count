"""imap.ini loading/saving and sender ignore-list rules."""
import configparser
import os
import re
from pathlib import Path

INI_FILENAME = "imap.ini"
INI_SECTION = "imap"
INI_KEYS = ("server", "username", "password", "folder", "ignore")
REQUIRED_INI_KEYS = ("server", "username", "password", "folder")


def ini_paths():
    here = Path(__file__).resolve().parent.parent / INI_FILENAME
    cwd = Path.cwd() / INI_FILENAME
    # Preserve order, remove duplicates.
    seen = set()
    paths = []
    for p in (here, cwd):
        if p not in seen:
            seen.add(p)
            paths.append(p)
    return paths


def load_ini():
    """Return (config_dict, path_used, error_message_or_None).

    config_dict has keys server/username/password/folder/ignore (ignore may
    be ''). If no INI exists, returns ({}, None, None). If an INI exists but
    is malformed or incomplete, returns (partial_dict, path, error).
    """
    for path in ini_paths():
        if path.is_file():
            parser = configparser.ConfigParser()
            try:
                parser.read(path, encoding="utf-8")
            except configparser.Error as exc:
                return {}, path, f"Could not parse {path}: {exc}"
            if INI_SECTION not in parser:
                return {}, path, f"{path} is missing the [{INI_SECTION}] section"
            section = parser[INI_SECTION]
            data = {k: section.get(k, "").strip() for k in INI_KEYS}
            missing = [k for k in REQUIRED_INI_KEYS if not data.get(k)]
            if missing:
                err = f"{path} is missing required key(s): {', '.join(missing)}"
                return data, path, err
            return data, path, None
    return {}, None, None


def save_ini(data: dict, path: Path) -> None:
    parser = configparser.ConfigParser()
    parser[INI_SECTION] = {k: data.get(k, "") for k in INI_KEYS}
    with open(path, "w") as fh:
        parser.write(fh)
    try:
        os.chmod(path, 0o600)
    except OSError:
        pass


def parse_ignore(raw: str):
    """Split a comma/whitespace list of entries; return list of lowercase rules.

    Each rule is either an exact email (contains '@') or a domain.
    Domain rules may start with '@' (e.g. '@example.com') or be a bare
    domain ('example.com'); both match any address whose domain part
    equals (or is a subdomain of) the rule.
    """
    if not raw:
        return []
    parts = re.split(r"[\s,]+", raw.strip())
    return [p.lower() for p in parts if p]


def make_ignore_predicate(rules):
    if not rules:
        return lambda email: False
    exact = set()
    domains = []
    for rule in rules:
        rule = rule.lower()
        if "@" in rule and not rule.startswith("@"):
            # Looks like a full email.
            exact.add(rule)
        else:
            domain = rule.lstrip("@")
            if domain:
                domains.append(domain)

    def predicate(email_addr: str) -> bool:
        e = email_addr.lower()
        if e in exact:
            return True
        if "@" in e:
            dom = e.split("@", 1)[1]
            for d in domains:
                if dom == d or dom.endswith("." + d):
                    return True
        return False

    return predicate
