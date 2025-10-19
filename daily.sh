#!/bin/bash
set -e
EXTRACT_DIR="/data/davsean/omicidx_root"

oidx sra extract $EXTRACT_DIR/sra 
oidx ebi-biosample extract $EXTRACT_DIR/ebi_biosample 
oidx biosample extract $EXTRACT_DIR/biosample 
oidx europepmc extract $EXTRACT_DIR/europepmc 
oidx geo extract 
