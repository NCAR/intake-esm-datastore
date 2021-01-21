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

touch long_name units standard_name
foreach i (`cat files`)
  set var = `basename $i | cut -f 1 -d .`
  ncdump -h $i | grep ${var}:long_name | cut -f 2 -d \" >> long_name
  ncdump -h $i | grep ${var}:units | cut -f 2 -d \" >> units
  ncdump -h $i | grep ${var}:standard_name | cut -f 2 -d \" >> standard_name
end

cat components | sed "c 1" > vertical_levels

cut -f 3,4 -d , components | tr , . > member_id
cut -f 2,3,4,7 -d , components | tr , ' ' | awk '{print $4,$3,$2,$1}' > source


set f = glade-na-cordex.csv

echo "variable,scenario,driver,rcm,frequency,grid,bias_correction,long_name,units,standard_name,vertical_levels,member_id,source,path" > $f

paste -d , components long_name units standard_name vertical_levels member_id source files >> $f

gzip $f

mv $f.gz $here

cd $here
