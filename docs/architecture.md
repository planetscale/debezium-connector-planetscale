<!--
#
# Copyright (c) 2025 James S. Clark
#
# This is private source code.  You may not use, copy, or distribute this file under any circumstances without written
# permission from the copyright holder, depicted above. All rights reserved.
#
-->
# Debezium Adapter for Planetscale

This repository implements a Debezium adapter for Planetscale, enabling change data capture (CDC) capabilities for
applications using Planetscale as their database. Planetscale is the Vitess company and cloud database; this repo adapts
the Debezium Vitess adapter for seamless integration with Planetscale.

In order to avoid upstream merge conflicts and achieve a minimal coupling surface, this repo forks the upstream adapter
**in bytecode**, effectively, and only overrides necessary logic to create a Planetscale adapter. Thus, upstream feature
updates and fixes can be adopted here as fast as possible and with minimal effort.

## Architecture

Coming soon.
