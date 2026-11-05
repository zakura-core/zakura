# Header-sync native mutations

Each directory holds one patch and a README naming the witness test that must
catch it. These patches have no planted case file or fuzz input yet, so they
live here. They move to `qa/planted/header-sync/<property>/` once the property
that consumes their fact registers.

Apply one patch at a time from a clean checkout, run its test, then restore
the source with `git apply -R`. A build error, a timeout, a panic or another
rule's failure doesn't count as detection.
