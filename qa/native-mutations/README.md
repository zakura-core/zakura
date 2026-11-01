# Native mutations

Each patch here plants a bug that a named test detects outside the planted
runner. The README beside each patch names that test and how to run it.

`cargo xtask planted` reads a case file and a committed fuzz input for every
patch under `qa/planted`. These patches have neither, so they live here.
