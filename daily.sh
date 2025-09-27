#!/bin/bash
set -e
EXTRACT_DIR="/Users/davsean/data/omicidx"

oidx sra extract $EXTRACT_DIR/sra
oidx ebi-biosample extract $EXTRACT_DIR/ebi_biosample
oidx geo extract 
oidx biosample extract $EXTRACT_DIR/biosample
oidx europepmc extract $EXTRACT_DIR/europepmc