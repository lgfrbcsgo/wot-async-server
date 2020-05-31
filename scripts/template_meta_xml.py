#!/usr/bin/python

import sys

content = """
<root>
    <id>lgfrbcsgo.async-server</id>
    <version>{version}</version>
    <name>Async Server</name>
    <description>A single threaded, non blocking TCP server for WoT mods which makes use of the `async` / `await`.</description>
</root>
"""

print(content.format(version=sys.argv[1]))
