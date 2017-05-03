#!/bin/bash

cargo rustdoc --open  --lib -- --no-defaults --passes collapse-docs --passes unindent-comments --passes strip-priv-imports
