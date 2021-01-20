#!/bin/tcsh


####
## Build CSV catalog for NA-CORDEX dataset

set here = `pwd`

cd `mktemp -d`


set root = /glade/collections/cdg/data/cordex/data

touch files
foreach branch (raw mbcn-gridMET mbcn-Daymet)
  find $root/$branch -type f -name \*nc >> files
end

cat files | xargs basename -a -s .nc | tr . , > components

touch longnames units
foreach i (`cat files`)
  set var = `basename $i | cut -f 1 -d .`
  ncdump -h $i | grep ${var}:long_name | cut -f 2 -d \" >> long_names
  ncdump -h $i | grep ${var}:units | cut -f 2 -d \" >> units
end

cat components | sed "c 1" > vertical_levels

# cut -f 6 -d , components | sed -e "s/NAM-\(11\|22\|44\)//; s/i/common/" > common

set f = glade-na-cordex.csv

echo "variable,scenario,driver,rcm,frequency,grid,bias_correction,long_name,units,vertical_levels,path" > $f

paste -d , components long_names units vertical_levels files >> $f

gzip $f

mv $f.gz $here

cd $here
